package tdb

import (
	"fmt"
	"sort"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/davecgh/go-spew/spew"
	"github.com/oxtoacart/bytemap"
	"github.com/oxtoacart/tdb/expr"
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
func (entry *Entry) Get(name string) expr.Value {
	if entry.fieldsIdx >= 0 {
		vals := entry.Fields[name]
		if vals == nil || entry.fieldsIdx >= len(vals) {
			return expr.Zero
		}
		return vals[entry.fieldsIdx]
	}
	rawVals := entry.rawValues[name]
	if rawVals != nil {
		if entry.valuesIdx < len(rawVals) {
			raw := rawVals[entry.valuesIdx]
			if entry.inIdx < len(raw) {
				return expr.Float(raw[entry.inIdx])
			}
		}
	}
	return expr.Zero
}

type QueryResult struct {
	Table      string
	From       time.Time
	To         time.Time
	Resolution time.Duration
	Fields     map[string]expr.Expr
	FieldOrder []string
	Dims       []string
	Entries    []*Entry
	Stats      *QueryStats
}

// TODO: if expressions repeat across fields, or across orderBy and having,
// optimize by sharing accumulators.

type Query struct {
	db           *DB
	table        string
	from         time.Time
	fromOffset   time.Duration
	to           time.Time
	toOffset     time.Duration
	offset       int
	limit        int
	resolution   time.Duration
	fields       map[string]expr.Expr
	fieldOrder   []string
	sortedFields sortedFields
	dims         []string
	dimsMap      map[string]bool
	filter       string
	orderBy      []expr.Expr
	having       expr.Cond
}

func (db *DB) SQLQuery(sql string) (*Query, error) {
	aq := &Query{db: db}
	err := aq.applySQL(sql)
	if err != nil {
		return nil, err
	}
	return aq, nil
}

func (db *DB) Query(table string) *Query {
	return &Query{db: db, table: table}
}

func (aq *Query) Resolution(resolution time.Duration) *Query {
	aq.resolution = resolution
	return aq
}

func (aq *Query) Select(name string, e expr.Expr) *Query {
	if aq.fields == nil {
		aq.fields = make(map[string]expr.Expr)
	}
	aq.fields[name] = e
	aq.fieldOrder = append(aq.fieldOrder, name)
	return aq
}

func (aq *Query) GroupBy(dim string) *Query {
	if aq.dims == nil {
		aq.dims = []string{dim}
	} else {
		aq.dims = append(aq.dims, dim)
	}
	return aq
}

func (aq *Query) OrderBy(e expr.Expr, asc bool) *Query {
	if !asc {
		e = expr.MULT(-1, e)
	}
	aq.orderBy = append(aq.orderBy, e)
	return aq
}

func (aq *Query) Having(e expr.Cond) *Query {
	aq.having = e
	return aq
}

func (aq *Query) Where(filter string) *Query {
	aq.filter = filter
	return aq
}

func (aq *Query) From(from time.Time) *Query {
	aq.from = from
	return aq
}

func (aq *Query) FromOffset(fromOffset time.Duration) *Query {
	aq.fromOffset = fromOffset
	return aq
}

func (aq *Query) To(to time.Time) *Query {
	aq.to = to
	return aq
}

func (aq *Query) ToOffset(toOffset time.Duration) *Query {
	aq.toOffset = toOffset
	return aq
}

func (aq *Query) Offset(offset int) *Query {
	aq.offset = offset
	return aq
}

func (aq *Query) Limit(limit int) *Query {
	aq.limit = limit
	return aq
}

func (aq *Query) Run() (*QueryResult, error) {
	q := &query{
		table:      aq.table,
		from:       aq.from,
		fromOffset: aq.fromOffset,
		to:         aq.to,
		toOffset:   aq.toOffset,
	}
	entries, err := aq.prepare(q)
	if err != nil {
		return nil, err
	}
	stats, err := aq.db.runQuery(q)
	if err != nil {
		return nil, err
	}
	log.Debug(spew.Sdump(stats))

	resultEntries, err := aq.buildEntries(entries)
	if err != nil {
		return nil, err
	}
	result := &QueryResult{
		Table:      aq.table,
		From:       q.from,
		To:         q.to,
		Resolution: aq.resolution,
		Fields:     aq.fields,
		FieldOrder: aq.fieldOrder,
		Dims:       aq.dims,
		Entries:    resultEntries,
		Stats:      stats,
	}
	if result.Dims == nil || len(result.Dims) == 0 {
		result.Dims = make([]string, 0, len(aq.dimsMap))
		for dim := range aq.dimsMap {
			result.Dims = append(result.Dims, dim)
		}
		sort.Strings(result.Dims)
	}
	return result, nil
}

func (aq *Query) prepare(q *query) (map[string]*Entry, error) {
	t := aq.db.getTable(q.table)
	if t == nil {
		return nil, fmt.Errorf("Table %v not found", q.table)
	}

	aq.sortedFields = sortFields(aq.fields)
	dependencies := make(map[string]bool, len(aq.sortedFields))
	for _, field := range aq.sortedFields {
		for _, dependency := range field.DependsOn() {
			dependencies[dependency] = true
		}
	}
	fields := make([]string, 0, len(dependencies))
	for dependency := range dependencies {
		fields = append(fields, dependency)
	}
	q.fields = fields

	if aq.filter != "" {
		log.Tracef("Applying filter: %v", aq.filter)
		filter, err := govaluate.NewEvaluableExpression(aq.filter)
		if err != nil {
			return nil, fmt.Errorf("Invalid filter expression: %v", err)
		}
		q.filter = filter
	}

	nativeResolution := t.resolution
	if aq.resolution == 0 {
		// Default to native resolution
		aq.resolution = nativeResolution
	}
	if aq.resolution < nativeResolution {
		return nil, fmt.Errorf("Query's resolution of %v is higher than table's native resolution of %v", aq.resolution, nativeResolution)
	}
	if aq.resolution%nativeResolution != 0 {
		return nil, fmt.Errorf("Query's resolution of %v is not evenly divisible by the table's native resolution of %v", aq.resolution, nativeResolution)
	}
	scalingFactor := int(aq.resolution / nativeResolution)
	log.Tracef("Scaling factor: %d", scalingFactor)
	// we'll calculate periods lazily later
	inPeriods := 0
	outPeriods := 0

	var sliceKey func(key bytemap.ByteMap) bytemap.ByteMap
	if len(aq.dims) == 0 {
		aq.dimsMap = make(map[string]bool, 0)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			cp := make([]byte, len(key))
			copy(cp, key)
			return cp
		}
	} else {
		sort.Strings(aq.dims)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return key.Slice(aq.dims...)
		}
	}

	entries := make(map[string]*Entry, 0)
	q.onValues = func(key bytemap.ByteMap, field string, vals []float64) {
		kb := sliceKey(key)
		entry := entries[string(kb)]
		if entry == nil {
			entry = &Entry{
				Dims:      key.AsMap(),
				Fields:    make(map[string][]expr.Accumulator, len(aq.fields)),
				rawValues: make(map[string][][]float64),
			}
			if len(aq.dims) == 0 {
				// Track dims
				for dim := range entry.Dims {
					aq.dimsMap[dim] = true
				}
			}
			// Initialize orderBys
			for _, orderBy := range aq.orderBy {
				entry.orderByValues = append(entry.orderByValues, orderBy.Accumulator())
			}
			// Initialize havings
			if aq.having != nil {
				entry.havingTest = aq.having.Accumulator()
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
		for _, field := range aq.sortedFields {
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
		for i := range aq.orderBy {
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

	if len(aq.orderBy) > 0 {
		sort.Sort(orderedEntries(result))
	}

	if aq.having != nil {
		unfiltered := result
		result = make([]*Entry, 0, len(result))
		for _, entry := range unfiltered {
			testResult := entry.havingTest.Get() == 1
			log.Tracef("Testing %v : %v", aq.having, testResult)
			if testResult {
				result = append(result, entry)
			}
		}
	}

	if aq.offset > 0 {
		offset := aq.offset
		if offset > len(result) {
			return make([]*Entry, 0), nil
		}
		result = result[offset:]
	}
	if aq.limit > 0 {
		end := aq.limit
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
