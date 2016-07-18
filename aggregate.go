package tdb

import (
	"fmt"
	"sort"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/davecgh/go-spew/spew"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
	"github.com/getlantern/tdb/sql"
)

type Entry struct {
	Dims          map[string]interface{}
	Fields        map[string][]expr.Accumulator
	NumPeriods    int
	scalingFactor int
	inPeriods     int
	numSamples    int
	inIdx         int
	valuesIdx     int
	fieldsIdx     int
	rawValues     map[string][][]float64
	orderByValues []expr.Accumulator
	havingTest    expr.Accumulator
}

// Get implements the method from interface govaluate.Parameters
func (entry *Entry) Get(name string) float64 {
	if entry.fieldsIdx >= 0 {
		vals := entry.Fields[name]
		if vals == nil || entry.fieldsIdx >= len(vals) {
			return 0
		}
		return vals[entry.fieldsIdx].Get()
	}
	rawVals := entry.rawValues[name]
	if rawVals != nil {
		if entry.valuesIdx < len(rawVals) {
			raw := rawVals[entry.valuesIdx]
			if entry.inIdx < len(raw) {
				return raw[entry.inIdx]
			}
		}
	}
	return 0
}

type QueryResult struct {
	Table      string
	AsOf       time.Time
	Until      time.Time
	Resolution time.Duration
	Fields     []sql.Field
	GroupBy    []string
	Entries    []*Entry
	Stats      *QueryStats
}

// TODO: if expressions repeat across fields, or across orderBy and having,
// optimize by sharing accumulators.

type Query struct {
	db *DB
	sql.Query
	dimsMap map[string]bool
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
	entries, err := aq.prepare(q)
	if err != nil {
		return nil, err
	}
	stats, err := aq.db.runQuery(q)
	if err != nil {
		return nil, err
	}
	if log.IsTraceEnabled() {
		log.Trace(spew.Sdump(stats))
	}

	resultEntries, err := aq.buildEntries(entries)
	if err != nil {
		return nil, err
	}
	result := &QueryResult{
		Table:      aq.From,
		AsOf:       q.asOf,
		Until:      q.until,
		Resolution: aq.Resolution,
		Fields:     aq.Fields,
		GroupBy:    aq.GroupBy,
		Entries:    resultEntries,
		Stats:      stats,
	}
	if result.GroupBy == nil || len(result.GroupBy) == 0 {
		result.GroupBy = make([]string, 0, len(aq.dimsMap))
		for dim := range aq.dimsMap {
			result.GroupBy = append(result.GroupBy, dim)
		}
		sort.Strings(result.GroupBy)
	}
	return result, nil
}

func (aq *Query) prepare(q *query) (map[string]*Entry, error) {
	t := aq.db.getTable(q.table)
	if t == nil {
		return nil, fmt.Errorf("Table %v not found", q.table)
	}

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
			return nil, fmt.Errorf("Invalid where expression: %v", err)
		}
		q.filter = where
	}

	nativeResolution := t.Resolution
	if aq.Resolution == 0 {
		log.Trace("Defaulting to native resolution")
		aq.Resolution = nativeResolution
	}
	if aq.Resolution > t.retentionPeriod {
		log.Trace("Not allowing resolution lower than retention period")
		aq.Resolution = t.retentionPeriod
	}
	if aq.Resolution < nativeResolution {
		return nil, fmt.Errorf("Query's resolution of %v is higher than table's native resolution of %v", aq.Resolution, nativeResolution)
	}
	if aq.Resolution%nativeResolution != 0 {
		return nil, fmt.Errorf("Query's resolution of %v is not evenly divisible by the table's native resolution of %v", aq.Resolution, nativeResolution)
	}
	scalingFactor := int(aq.Resolution / nativeResolution)
	log.Tracef("Scaling factor: %d", scalingFactor)
	// we'll calculate periods lazily later
	inPeriods := 0
	outPeriods := 0

	var sliceKey func(key bytemap.ByteMap) bytemap.ByteMap
	if len(aq.GroupBy) == 0 {
		aq.dimsMap = make(map[string]bool, 0)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			// Original key is fine
			return key
		}
	} else {
		groupBy := make([]string, len(aq.GroupBy))
		copy(groupBy, aq.GroupBy)
		sort.Strings(groupBy)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return key.Slice(groupBy...)
		}
	}

	entries := make(map[string]*Entry, 0)
	q.onValues = func(key bytemap.ByteMap, field string, vals []float64) {
		kb := sliceKey(key)
		entry := entries[string(kb)]
		if entry == nil {
			entry = &Entry{
				Dims:      key.AsMap(),
				Fields:    make(map[string][]expr.Accumulator, len(aq.Fields)),
				rawValues: make(map[string][][]float64),
			}
			if len(aq.GroupBy) == 0 {
				// Track dims
				for dim := range entry.Dims {
					aq.dimsMap[dim] = true
				}
			}
			// Initialize orderBys
			for _, orderBy := range aq.OrderBy {
				entry.orderByValues = append(entry.orderByValues, orderBy.Accumulator())
			}
			// Initialize havings
			if aq.Having != nil {
				entry.havingTest = aq.Having.Accumulator()
			}
			entries[string(kb)] = entry
		}
		if entry.scalingFactor == 0 {
			if inPeriods < 1 {
				inPeriods = len(vals)
				// Limit inPeriods based on what we can fit into outPeriods
				inPeriods -= inPeriods % scalingFactor
				outPeriods = (inPeriods / scalingFactor) + 1
				inPeriods = outPeriods * scalingFactor
				log.Tracef("In: %d   Out: %d", inPeriods, outPeriods)
			}
			entry.NumPeriods = outPeriods
			entry.inPeriods = inPeriods
			entry.scalingFactor = scalingFactor
		}
		rawValues := entry.rawValues[field]
		if rawValues == nil {
			rawValues = [][]float64{vals}
		} else {
			rawValues = append(rawValues, vals)
		}
		entry.rawValues[field] = rawValues
		if len(rawValues) > entry.numSamples {
			entry.numSamples = len(rawValues)
		}
	}

	return entries, nil
}

func (aq *Query) buildEntries(entries map[string]*Entry) ([]*Entry, error) {
	result := make([]*Entry, 0, len(entries))
	if len(entries) == 0 {
		return result, nil
	}

	for _, entry := range entries {
		// Don't get fields right now
		entry.fieldsIdx = -1
		for _, field := range aq.Fields {
			// Initialize accumulators
			vals := make([]expr.Accumulator, 0, entry.NumPeriods)
			for i := 0; i < entry.NumPeriods; i++ {
				vals = append(vals, field.Accumulator())
			}
			entry.Fields[field.Name] = vals

			// Calculate per-period values
			for i := 0; i < entry.inPeriods; i++ {
				entry.inIdx = i
				outIdx := i / entry.scalingFactor
				for j := 0; j < entry.numSamples; j++ {
					entry.valuesIdx = j
					vals[outIdx].Update(entry)
				}
			}
		}

		// Calculate order bys
		for i := range aq.OrderBy {
			accum := entry.orderByValues[i]
			for j := 0; j < entry.inPeriods; j++ {
				entry.fieldsIdx = j
				accum.Update(entry)
			}
		}

		// Calculate havings
		if entry.havingTest != nil {
			for j := 0; j < entry.inPeriods; j++ {
				entry.fieldsIdx = j
				entry.havingTest.Update(entry)
			}
		}

		result = append(result, entry)
	}

	if len(aq.OrderBy) > 0 {
		sort.Sort(orderedEntries(result))
	}

	if aq.Having != nil {
		unfiltered := result
		result = make([]*Entry, 0, len(result))
		for _, entry := range unfiltered {
			testResult := entry.havingTest.Get() == 1
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

type orderedEntries []*Entry

func (r orderedEntries) Len() int      { return len(r) }
func (r orderedEntries) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r orderedEntries) Less(i, j int) bool {
	a := r[i]
	b := r[j]
	for i := 0; i < len(a.orderByValues); i++ {
		diff := a.orderByValues[i].Get() - b.orderByValues[i].Get()
		if diff < 0 {
			return true
		}
		if diff > 0 {
			return false
		}
	}
	return false
}
