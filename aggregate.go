package tibsdb

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/tibsdb/expr"
	"github.com/getlantern/tibsdb/sql"
)

type Entry struct {
	Dims          map[string]interface{}
	Fields        []sequence
	q             *Query
	fieldsIdx     int
	orderByValues [][]byte
	havingTest    []byte
}

// Get implements the method from interface govaluate.Parameters
func (entry *Entry) Get(name string) (float64, bool) {
	if entry.fieldsIdx >= 0 {
		return entry.value(name, entry.fieldsIdx)
	}
	return 0, false
}

func (entry *Entry) Value(name string, period int) float64 {
	v, _ := entry.value(name, period)
	return v
}

func (entry *Entry) value(name string, period int) (float64, bool) {
	var field sql.Field
	var seq sequence
	for i, candidate := range entry.q.Fields {
		if candidate.Name == name {
			field = candidate
			seq = entry.Fields[i]
			break
		}
	}
	if seq == nil || period > seq.numPeriods(field.EncodedWidth()) {
		return 0, false
	}
	return seq.ValueAt(period, field)
}

type QueryResult struct {
	Table         string
	AsOf          time.Time
	Until         time.Time
	Resolution    time.Duration
	Fields        []sql.Field
	GroupBy       []string
	Entries       []*Entry
	Stats         *QueryStats
	NumPeriods    int
	ScannedPoints int64
}

// TODO: if expressions repeat across fields, or across orderBy and having,
// optimize by sharing accumulators.

type Query struct {
	db *DB
	sql.Query
	dimsMap       map[string]bool
	scalingFactor int
	inPeriods     int
	outPeriods    int
}

func (db *DB) SQLQuery(sqlString string) (*Query, error) {
	query, err := sql.Parse(sqlString)
	if err != nil {
		return nil, err
	}
	return db.Query(query), nil
}

func (db *DB) Query(query *sql.Query) *Query {
	return &Query{db: db, Query: *query}
}

func (aq *Query) Run() (*QueryResult, error) {
	q := &query{
		table:       aq.From,
		asOf:        aq.AsOf,
		asOfOffset:  aq.AsOfOffset,
		until:       aq.Until,
		untilOffset: aq.UntilOffset,
	}
	responses, entriesCh, wg, scannedPoints, err := aq.prepare(q)
	if err != nil {
		return nil, err
	}
	stats, err := q.run(aq.db)
	if err != nil {
		return nil, err
	}
	close(responses)
	wg.Wait()
	close(entriesCh)
	var entries []map[string]*Entry
	for e := range entriesCh {
		entries = append(entries, e)
	}
	// Merge entries
	entriesOut := make(map[string]*Entry)
	for i, e := range entries {
		for k, v := range e {
			for j := i; j < len(entries); j++ {
				o := entries[j]
				if j != i {
					vo, ok := o[k]
					if ok {
						for x, os := range vo.Fields {
							v.Fields[x] = v.Fields[x].merge(os, aq.Fields[x], aq.Resolution, aq.AsOf)
						}
						delete(o, k)
					}
				}
			}
			entriesOut[k] = v
		}
	}
	// if log.IsTraceEnabled() {
	log.Debugf("%v\nScanned Points: %v", spew.Sdump(stats), humanize.Comma(*scannedPoints))
	// }

	resultEntries, err := aq.buildEntries(q, entriesOut)
	if err != nil {
		return nil, err
	}
	result := &QueryResult{
		Table:         aq.From,
		AsOf:          q.asOf,
		Until:         q.until,
		Resolution:    aq.Resolution,
		Fields:        aq.Fields,
		GroupBy:       aq.GroupBy,
		NumPeriods:    aq.outPeriods,
		Entries:       resultEntries,
		Stats:         stats,
		ScannedPoints: *scannedPoints,
	}
	if len(result.GroupBy) == 0 {
		result.GroupBy = make([]string, 0, len(aq.dimsMap))
		for dim := range aq.dimsMap {
			result.GroupBy = append(result.GroupBy, dim)
		}
		sort.Strings(result.GroupBy)
	}

	return result, nil
}

type queryResponse struct {
	key         bytemap.ByteMap
	field       string
	e           expr.Expr
	seq         sequence
	startOffset int
}

func (aq *Query) prepare(q *query) (chan *queryResponse, chan map[string]*Entry, *sync.WaitGroup, *int64, error) {
	// Figure out field dependencies
	sortedFields := make(sortedFields, len(aq.Fields))
	copy(sortedFields, aq.Fields)
	sort.Sort(sortedFields)
	dependencies := make(map[string]bool, len(sortedFields))
	for _, field := range sortedFields {
		for _, dependency := range field.DependsOn() {
			dependencies[dependency] = true
		}
	}
	fields := make([]string, 0, len(dependencies))
	for dependency := range dependencies {
		fields = append(fields, dependency)
	}
	q.fields = fields

	if aq.Where != "" {
		log.Tracef("Applying where: %v", aq.Where)
		where, err := govaluate.NewEvaluableExpression(aq.Where)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("Invalid where expression: %v", err)
		}
		q.filter = where
	}

	err := q.init(aq.db)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	t := q.t
	nativeResolution := t.Resolution
	if aq.Resolution == 0 {
		log.Trace("Defaulting to native resolution")
		aq.Resolution = nativeResolution
	}
	if aq.Resolution > t.RetentionPeriod {
		log.Trace("Not allowing resolution lower than retention period")
		aq.Resolution = t.RetentionPeriod
	}
	if aq.Resolution < nativeResolution {
		return nil, nil, nil, nil, fmt.Errorf("Query's resolution of %v is higher than table's native resolution of %v", aq.Resolution, nativeResolution)
	}
	if aq.Resolution%nativeResolution != 0 {
		return nil, nil, nil, nil, fmt.Errorf("Query's resolution of %v is not evenly divisible by the table's native resolution of %v", aq.Resolution, nativeResolution)
	}

	aq.scalingFactor = int(aq.Resolution / nativeResolution)
	log.Tracef("Scaling factor: %d", aq.scalingFactor)

	aq.inPeriods = int(q.until.Sub(q.asOf) / nativeResolution)
	// Limit inPeriods based on what we can fit into outPeriods
	aq.inPeriods -= aq.inPeriods % aq.scalingFactor
	aq.outPeriods = (aq.inPeriods / aq.scalingFactor) + 1
	aq.inPeriods = aq.outPeriods * aq.scalingFactor
	log.Tracef("In: %d   Out: %d", aq.inPeriods, aq.outPeriods)

	var dimsMapMutex sync.Mutex
	var sliceKey func(key bytemap.ByteMap) bytemap.ByteMap
	if aq.GroupByAll {
		// Use all original dimensions in grouping
		aq.dimsMap = make(map[string]bool, 0)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			// Original key is fine
			return key
		}
	} else if len(aq.GroupBy) == 0 {
		defaultKey := bytemap.New(map[string]interface{}{
			"default": "",
		})
		// No grouping, put everything under a single key
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return defaultKey
		}
	} else {
		groupBy := make([]string, len(aq.GroupBy))
		copy(groupBy, aq.GroupBy)
		sort.Strings(groupBy)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return key.Slice(groupBy...)
		}
	}

	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	responses := make(chan *queryResponse, numWorkers)
	entriesCh := make(chan map[string]*Entry, numWorkers)
	scannedPoints := int64(0)

	worker := func() {
		entries := make(map[string]*Entry, 0)
		sfp := &singleFieldParams{}
		for resp := range responses {
			kb := sliceKey(resp.key)
			entry := entries[string(kb)]
			if entry == nil {
				entry = &Entry{
					Dims:   kb.AsMap(),
					Fields: make([]sequence, len(aq.Fields)),
					q:      aq,
				}
				if aq.GroupByAll {
					// Track dims
					for dim := range entry.Dims {
						// TODO: instead of locking on this shared state, have the workers
						// return their own dimsMaps and merge them
						dimsMapMutex.Lock()
						aq.dimsMap[dim] = true
						dimsMapMutex.Unlock()
					}
				}

				// Initialize fields
				for i, f := range aq.Fields {
					seq := newSequence(f.EncodedWidth(), aq.outPeriods)
					seq.setStart(q.until)
					entry.Fields[i] = seq
				}

				// Initialize havings
				if aq.Having != nil {
					entry.havingTest = make([]byte, aq.Having.EncodedWidth())
				}

				// Initialize order bys
				for _, e := range aq.OrderBy {
					entry.orderByValues = append(entry.orderByValues, make([]byte, e.EncodedWidth()))
				}
				entries[string(kb)] = entry
			}

			sfp.field = resp.field
			inPeriods := resp.seq.numPeriods(resp.e.EncodedWidth()) - resp.startOffset
			for i := 0; i < inPeriods && i < aq.inPeriods; i++ {
				val, wasSet := resp.seq.ValueAt(i+resp.startOffset, resp.e)
				if wasSet {
					atomic.AddInt64(&scannedPoints, 1)
					sfp.value = val
					out := i / aq.scalingFactor
					for j, seq := range entry.Fields {
						seq.updateValueAt(out, aq.Fields[j], sfp)
					}
				}
			}
		}

		entriesCh <- entries
		wg.Done()
	}

	for i := 0; i < numWorkers; i++ {
		go worker()
	}

	q.onValues = func(key bytemap.ByteMap, field string, e expr.Expr, seq sequence, startOffset int) {
		responses <- &queryResponse{key, field, e, seq, startOffset}
	}

	return responses, entriesCh, &wg, &scannedPoints, nil
}

func (aq *Query) buildEntries(q *query, entries map[string]*Entry) ([]*Entry, error) {
	result := make([]*Entry, 0, len(entries))
	if len(entries) == 0 {
		return result, nil
	}

	for _, entry := range entries {
		// Don't get fields right now
		entry.fieldsIdx = -1

		// Calculate order bys
		for i, e := range aq.OrderBy {
			b := entry.orderByValues[i]
			for j := 0; j < aq.inPeriods; j++ {
				entry.fieldsIdx = j
				e.Update(b, entry)
			}
		}

		// Calculate havings
		if entry.havingTest != nil {
			for j := 0; j < aq.inPeriods; j++ {
				entry.fieldsIdx = j
				aq.Having.Update(entry.havingTest, entry)
			}
		}

		result = append(result, entry)
	}

	if len(aq.OrderBy) > 0 {
		sort.Sort(orderedEntries{aq.OrderBy, result})
	}

	if aq.Having != nil {
		unfiltered := result
		result = make([]*Entry, 0, len(result))
		for _, entry := range unfiltered {
			_testResult, _, _ := aq.Having.Get(entry.havingTest)
			testResult := _testResult == 1
			log.Tracef("Testing %v : %v", aq.Having, testResult)
			if testResult {
				result = append(result, entry)
			}
		}
	}

	if aq.Offset > 0 {
		offset := aq.Offset
		if offset > len(result) {
			return make([]*Entry, 0), nil
		}
		result = result[offset:]
	}
	if aq.Limit > 0 {
		end := aq.Limit
		if end > len(result) {
			end = len(result)
		}
		result = result[:end]
	}

	return result, nil
}

type orderedEntries struct {
	orderBy []expr.Expr
	entries []*Entry
}

func (r orderedEntries) Len() int      { return len(r.entries) }
func (r orderedEntries) Swap(i, j int) { r.entries[i], r.entries[j] = r.entries[j], r.entries[i] }
func (r orderedEntries) Less(i, j int) bool {
	a := r.entries[i]
	b := r.entries[j]
	for o := 0; o < len(a.orderByValues); o++ {
		orderBy := r.orderBy[o]
		x, _, _ := orderBy.Get(a.orderByValues[o])
		y, _, _ := orderBy.Get(b.orderByValues[o])
		diff := x - y
		if diff < 0 {
			return true
		}
		if diff > 0 {
			return false
		}
	}
	return false
}

type singleFieldParams struct {
	field string
	value float64
}

func (sfp singleFieldParams) Get(field string) (float64, bool) {
	if field == sfp.field {
		return sfp.value, true
	}
	return 0, false
}
