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
	orderByValues [][]byte
	havingTest    []byte
}

type QueryResult struct {
	Table         string
	AsOf          time.Time
	Until         time.Time
	Resolution    time.Duration
	Fields        []sql.Field
	FieldNames    []string // FieldNames are needed for serializing QueryResult across rpc
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
}

type queryResponse struct {
	key         bytemap.ByteMap
	field       string
	e           expr.Expr
	seq         sequence
	startOffset int
}

type subMergeSpec struct {
	idx      int
	field    sql.Field
	subMerge expr.SubMerge
}

type queryExecution struct {
	db *DB
	sql.Query
	t                *table
	q                *query
	subMergers       [][]expr.SubMerge
	havingSubMergers []expr.SubMerge
	dimsMap          map[string]bool
	scalingFactor    int
	inPeriods        int
	outPeriods       int
	numWorkers       int
	responsesCh      chan *queryResponse
	entriesCh        chan map[string]*Entry
	wg               sync.WaitGroup
	scannedPoints    int64
}

func (db *DB) SQLQuery(sqlString string) (*Query, error) {
	table, err := sql.TableFor(sqlString)
	if err != nil {
		return nil, err
	}
	t := db.getTable(table)
	if t == nil {
		return nil, fmt.Errorf("Table '%v' not found", table)
	}

	query, err := sql.Parse(sqlString, t.Fields...)
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
	numWorkers := runtime.NumCPU()
	numWorkers = 1
	exec := &queryExecution{
		Query:       aq.Query,
		db:          aq.db,
		t:           aq.db.getTable(aq.From),
		q:           q,
		numWorkers:  numWorkers,
		responsesCh: make(chan *queryResponse, numWorkers),
		entriesCh:   make(chan map[string]*Entry, numWorkers),
	}
	exec.wg.Add(numWorkers)
	return exec.run()
}

func (exec *queryExecution) run() (*QueryResult, error) {
	err := exec.prepare()
	if err != nil {
		return nil, err
	}
	return exec.finish()
}

func (exec *queryExecution) prepare() error {
	// Figure out what to select
	var subMergers [][]expr.SubMerge
	columns := make([]expr.Expr, 0, len(exec.t.Fields))
	for _, column := range exec.t.Fields {
		columns = append(columns, column.Expr)
	}
	includedColumns := make(map[int]bool)
	for _, field := range exec.Fields {
		sms := field.Expr.SubMergers(columns)
		subMergers = append(subMergers, sms)
		for j, sm := range sms {
			if sm != nil {
				includedColumns[j] = true
			}
		}
	}

	var havingSubMergers []expr.SubMerge
	if exec.Having != nil {
		sms := exec.Having.SubMergers(columns)
		for j, sm := range sms {
			if sm != nil {
				includedColumns[j] = true
			}
		}
	}

	var fields []string
	for i, column := range exec.t.Fields {
		if includedColumns[i] {
			fields = append(fields, column.Name)
		}
	}
	exec.q.fields = fields
	exec.subMergers = subMergers
	exec.havingSubMergers = havingSubMergers

	if exec.Where != "" {
		log.Tracef("Applying where: %v", exec.Where)
		where, err := govaluate.NewEvaluableExpression(exec.Where)
		if err != nil {
			return fmt.Errorf("Invalid where expression: %v", err)
		}
		exec.q.filter = where
	}

	err := exec.q.init(exec.db)
	if err != nil {
		return err
	}

	nativeResolution := exec.t.Resolution
	if exec.Resolution == 0 {
		log.Trace("Defaulting to native resolution")
		exec.Resolution = nativeResolution
	}
	if exec.Resolution > exec.t.RetentionPeriod {
		log.Trace("Not allowing resolution lower than retention period")
		exec.Resolution = exec.t.RetentionPeriod
	}
	if exec.Resolution < nativeResolution {
		return fmt.Errorf("Query's resolution of %v is higher than table's native resolution of %v", exec.Resolution, nativeResolution)
	}
	if exec.Resolution%nativeResolution != 0 {
		return fmt.Errorf("Query's resolution of %v is not evenly divisible by the table's native resolution of %v", exec.Resolution, nativeResolution)
	}

	exec.scalingFactor = int(exec.Resolution / nativeResolution)
	log.Tracef("Scaling factor: %d", exec.scalingFactor)

	exec.inPeriods = int(exec.q.until.Sub(exec.q.asOf) / nativeResolution)
	// Limit inPeriods based on what we can fit into outPeriods
	exec.inPeriods -= exec.inPeriods % exec.scalingFactor
	exec.outPeriods = exec.inPeriods / exec.scalingFactor
	if exec.outPeriods == 0 {
		exec.outPeriods = 1
	}
	exec.inPeriods = exec.outPeriods * exec.scalingFactor
	log.Tracef("In: %d   Out: %d", exec.inPeriods, exec.outPeriods)

	var dimsMapMutex sync.Mutex
	var sliceKey func(key bytemap.ByteMap) bytemap.ByteMap
	if exec.GroupByAll {
		// Use all original dimensions in grouping
		exec.dimsMap = make(map[string]bool, 0)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			// Original key is fine
			return key
		}
	} else if len(exec.GroupBy) == 0 {
		defaultKey := bytemap.New(map[string]interface{}{
			"default": "",
		})
		// No grouping, put everything under a single key
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return defaultKey
		}
	} else {
		groupBy := make([]string, len(exec.GroupBy))
		copy(groupBy, exec.GroupBy)
		sort.Strings(groupBy)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return key.Slice(groupBy...)
		}
	}

	worker := func() {
		entries := make(map[string]*Entry, 0)
		for resp := range exec.responsesCh {
			kb := sliceKey(resp.key)
			entry := entries[string(kb)]
			if entry == nil {
				entry = &Entry{
					Dims:   kb.AsMap(),
					Fields: make([]sequence, len(exec.Fields)),
				}
				if exec.GroupByAll {
					// Track dims
					for dim := range entry.Dims {
						// TODO: instead of locking on this shared state, have the workers
						// return their own dimsMaps and merge them
						dimsMapMutex.Lock()
						exec.dimsMap[dim] = true
						dimsMapMutex.Unlock()
					}
				}

				// Initialize fields
				for i, f := range exec.Fields {
					seq := newSequence(f.Expr.EncodedWidth(), exec.outPeriods)
					seq.setStart(exec.q.until)
					entry.Fields[i] = seq
				}

				// Initialize havings
				if exec.Having != nil {
					entry.havingTest = make([]byte, exec.Having.EncodedWidth())
				}

				// Initialize order bys
				for _, e := range exec.OrderBy {
					entry.orderByValues = append(entry.orderByValues, make([]byte, e.EncodedWidth()))
				}
				entries[string(kb)] = entry
			}

			inPeriods := resp.seq.numPeriods(resp.e.EncodedWidth()) - resp.startOffset
			for c, column := range exec.t.Fields {
				if column.Name != resp.field {
					continue
				}
				for t := 0; t < inPeriods && t < exec.inPeriods; t++ {
					other, wasSet := resp.seq.dataAt(t+resp.startOffset, resp.e)
					if !wasSet {
						continue
					}

					for f, field := range exec.Fields {
						subMerge := exec.subMergers[f][c]
						if subMerge == nil {
							continue
						}
						seq := entry.Fields[f]
						atomic.AddInt64(&exec.scannedPoints, 1)
						out := t / exec.scalingFactor
						log.Debugf("Merging %v into %v", column, field)
						seq.subMergeValueAt(out, field.Expr, subMerge, other)
						val, _ := seq.ValueAt(out, field.Expr)
						log.Debugf("Yielded %f", val)
					}

					// Calculate havings
					if exec.Having != nil {
						subMerge := exec.havingSubMergers[c]
						subMerge(entry.havingTest, other)
					}
				}
			}
		}

		exec.entriesCh <- entries
		exec.wg.Done()
	}

	for i := 0; i < exec.numWorkers; i++ {
		go worker()
	}

	exec.q.onValues = func(key bytemap.ByteMap, field string, e expr.Expr, seq sequence, startOffset int) {
		exec.responsesCh <- &queryResponse{key, field, e, seq, startOffset}
	}

	return nil
}

func (exec *queryExecution) finish() (*QueryResult, error) {
	stats, err := exec.q.run(exec.db)
	if err != nil {
		return nil, err
	}
	close(exec.responsesCh)
	exec.wg.Wait()
	close(exec.entriesCh)
	var entries []map[string]*Entry
	for e := range exec.entriesCh {
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
							v.Fields[x] = v.Fields[x].merge(os, exec.Fields[x].Expr, exec.Resolution, exec.AsOf)
						}
						delete(o, k)
					}
					if exec.Having != nil {
						exec.Having.Merge(v.havingTest, v.havingTest, vo.havingTest)
					}
				}

			}
			entriesOut[k] = v
		}
	}
	// if log.IsTraceEnabled() {
	log.Debugf("%v\nScanned Points: %v", spew.Sdump(stats), humanize.Comma(exec.scannedPoints))
	// }

	resultEntries, err := exec.buildEntries(entriesOut)
	if err != nil {
		return nil, err
	}
	result := &QueryResult{
		Table:         exec.From,
		AsOf:          exec.q.asOf,
		Until:         exec.q.until,
		Resolution:    exec.Resolution,
		Fields:        exec.Fields,
		FieldNames:    make([]string, 0, len(exec.Fields)),
		GroupBy:       exec.GroupBy,
		NumPeriods:    exec.outPeriods,
		Entries:       resultEntries,
		Stats:         stats,
		ScannedPoints: exec.scannedPoints,
	}
	for _, field := range exec.Fields {
		result.FieldNames = append(result.FieldNames, field.Name)
	}
	if len(result.GroupBy) == 0 {
		result.GroupBy = make([]string, 0, len(exec.dimsMap))
		for dim := range exec.dimsMap {
			result.GroupBy = append(result.GroupBy, dim)
		}
		sort.Strings(result.GroupBy)
	}

	return result, nil
}

func (exec *queryExecution) buildEntries(entries map[string]*Entry) ([]*Entry, error) {
	result := make([]*Entry, 0, len(entries))
	if len(entries) == 0 {
		return result, nil
	}

	for _, entry := range entries {
		// // Calculate order bys
		// for i, e := range exec.OrderBy {
		// 	b := entry.orderByValues[i]
		// 	for j := 0; j < exec.inPeriods; j++ {
		// 		entry.fieldsIdx = j
		// 		e.Update(b, entry)
		// 	}
		// }

		result = append(result, entry)
	}

	if len(exec.OrderBy) > 0 {
		sort.Sort(orderedEntries{exec.OrderBy, result})
	}

	if exec.Having != nil {
		unfiltered := result
		result = make([]*Entry, 0, len(result))
		for _, entry := range unfiltered {
			_testResult, _, _ := exec.Having.Get(entry.havingTest)
			testResult := _testResult == 1
			log.Tracef("Testing %v : %v", exec.Having, testResult)
			if testResult {
				result = append(result, entry)
			}
		}
	}

	if exec.Offset > 0 {
		offset := exec.Offset
		if offset > len(result) {
			return make([]*Entry, 0), nil
		}
		result = result[offset:]
	}
	if exec.Limit > 0 {
		end := exec.Limit
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
