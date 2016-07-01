package tdb

import (
	"fmt"
	"sort"
	"time"

	"github.com/oxtoacart/tdb/expr"
)

type Entry struct {
	Dims          map[string]interface{}
	Fields        map[string][]expr.Accumulator
	Totals        map[string]expr.Accumulator
	NumPeriods    int
	scalingFactor int
	inPeriods     int
	numSamples    int
	inIdx         int
	valuesIdx     int
	rawValues     map[string][][]float64
}

// Get implements the method from interface govaluate.Parameters
func (entry *Entry) Get(name string) expr.Value {
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

type Query struct {
	db           *DB
	table        string
	from         time.Time
	to           time.Time
	resolution   time.Duration
	fields       map[string]expr.Expr
	sortedFields sortedFields
	dims         []string
	orderBy      map[string]bool
}

type QueryResult struct {
	Table      string
	From       time.Time
	To         time.Time
	Resolution time.Duration
	Fields     []string
	Dims       []string
	Entries    []*Entry
}

func (db *DB) Query(table string, resolution time.Duration) *Query {
	return &Query{db: db, table: table, resolution: resolution}
}

func (aq *Query) Select(name string, e expr.Expr) *Query {
	if aq.fields == nil {
		aq.fields = make(map[string]expr.Expr)
	}
	aq.fields[name] = e
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

func (aq *Query) OrderBy(name string, asc bool) *Query {
	if aq.orderBy == nil {
		aq.orderBy = make(map[string]bool)
	}
	aq.orderBy[name] = asc
	return aq
}

func (aq *Query) From(from time.Time) *Query {
	aq.from = from
	return aq
}

func (aq *Query) To(to time.Time) *Query {
	aq.to = to
	return aq
}

func (aq *Query) Run() (*QueryResult, error) {
	q := &query{
		table: aq.table,
		from:  aq.from,
		to:    aq.to,
	}
	entries, err := aq.prepare(q)
	if err != nil {
		return nil, err
	}
	err = aq.db.runQuery(q)
	if err != nil {
		return nil, err
	}
	resultEntries, err := aq.buildEntries(entries)
	if err != nil {
		return nil, err
	}
	return &QueryResult{
		Entries: resultEntries,
	}, nil
}

func (aq *Query) prepare(q *query) (map[string]*Entry, error) {
	t := aq.db.getTable(q.table)
	if t == nil {
		return nil, fmt.Errorf("Table %v not found", q.table)
	}

	if aq.orderBy != nil {
		for orderField := range aq.orderBy {
			if aq.fields[orderField] == nil {
				return nil, fmt.Errorf("OrderBy field %v is not included in selected fields", orderField)
			}
		}
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

	nativeResolution := t.resolution
	resolution := aq.resolution
	if resolution == 0 {
		// Default to native resolution
		resolution = nativeResolution
	}
	if resolution < nativeResolution {
		return nil, fmt.Errorf("Query's resolution of %v is higher than table's native resolution of %v", resolution, nativeResolution)
	}
	if resolution%nativeResolution != 0 {
		return nil, fmt.Errorf("Query's resolution of %v is not evenly divisible by the table's native resolution of %v", resolution, nativeResolution)
	}
	scalingFactor := int(resolution / nativeResolution)
	// we'll calculate periods lazily later
	inPeriods := 0
	outPeriods := 0

	includedDims := make(map[string]bool, len(aq.dims))
	for _, dim := range aq.dims {
		includedDims[dim] = true
	}
	includeDim := func(dim string) bool {
		return includedDims[dim]
	}

	entries := make(map[string]*Entry, 0)
	q.onValues = func(key map[string]interface{}, field string, vals []float64) {
		// Trim key down only to included dims
		for k := range key {
			if !includeDim(k) {
				delete(key, k)
			}
		}
		kb, err := keyToBytes(key)
		if err != nil {
			log.Errorf("Unable to encode key, skipping: %v", err)
			return
		}
		ks := string(kb)
		entry := entries[ks]
		if entry == nil {
			entry = &Entry{
				Dims:      key,
				Fields:    make(map[string][]expr.Accumulator, len(aq.fields)),
				Totals:    make(map[string]expr.Accumulator, len(aq.fields)),
				rawValues: make(map[string][][]float64),
			}
			entries[ks] = entry
		}
		if entry.scalingFactor == 0 {
			if inPeriods < 1 {
				inPeriods = len(vals)
				// Limit inPeriods based on what we can fit into outPeriods
				inPeriods -= inPeriods % scalingFactor
				outPeriods = (inPeriods / scalingFactor) + 1
				inPeriods = outPeriods * scalingFactor
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
		for _, field := range aq.sortedFields {
			// Initialize accumulators
			vals := make([]expr.Accumulator, 0, entry.NumPeriods)
			for i := 0; i < entry.NumPeriods; i++ {
				vals = append(vals, field.Accumulator())
			}
			entry.Fields[field.Name] = vals
			total := field.Accumulator()
			entry.Totals[field.Name] = total

			// Calculate per-period values
			for i := 0; i < entry.inPeriods; i++ {
				entry.inIdx = i
				outIdx := i / entry.scalingFactor
				for j := 0; j < entry.numSamples; j++ {
					entry.valuesIdx = j
					vals[outIdx].Update(entry)
					total.Update(entry)
				}
			}
		}

		result = append(result, entry)
	}

	if aq.orderBy != nil && len(aq.orderBy) > 0 {
		sort.Sort(&orderedEntries{result, aq.orderBy})
	}

	return result, nil
}

type orderedEntries struct {
	r       []*Entry
	orderBy map[string]bool
}

func (r *orderedEntries) Len() int      { return len(r.r) }
func (r *orderedEntries) Swap(i, j int) { r.r[i], r.r[j] = r.r[j], r.r[i] }
func (r *orderedEntries) Less(i, j int) bool {
	a := r.r[i]
	b := r.r[j]
	for field, asc := range r.orderBy {
		fa := a.Totals[field].Get()
		fb := b.Totals[field].Get()
		if fa == fb {
			continue
		}
		if asc {
			return fb < fa
		}
		return fa > fb
	}
	return false
}
