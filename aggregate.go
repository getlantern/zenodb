package tdb

import (
	"fmt"
	"sort"
	"time"

	"github.com/oxtoacart/tdb/expr"
)

const (
	ORDER_ASC  = 1
	ORDER_DESC = 2
)

type Order int

type AggregateQuery struct {
	Resolution      time.Duration
	Dims            []string
	Fields          map[string]expr.Expr
	sortedFields    sortedFields
	Summaries       map[string]expr.Expr
	sortedSummaries sortedFields
	OrderBy         map[string]Order
}

type AggregateEntry struct {
	Dims          map[string]interface{}
	Fields        map[string][]expr.Accumulator
	Summaries     map[string]expr.Accumulator
	NumPeriods    int
	scalingFactor int
	inPeriods     int
	numSamples    int
	inIdx         int
	valuesIdx     int
	outIdx        int
	rawValues     map[string][][]float64
}

// Get implements the method from interface govaluate.Parameters
func (entry *AggregateEntry) Get(name string) expr.Value {
	rawVals := entry.rawValues[name]
	if rawVals != nil {
		raw := rawVals[entry.valuesIdx]
		if entry.inIdx >= len(raw) {
			return expr.Zero
		}
		return expr.Float(raw[entry.inIdx])
	}
	vals := entry.Fields[name]
	if vals != nil {
		return vals[entry.outIdx]
	}
	return expr.Zero
}

func (aq *AggregateQuery) Run(db *DB, q *Query) ([]*AggregateEntry, error) {
	aq.sortedFields = sortFields(aq.Fields)
	aq.sortedSummaries = sortFields(aq.Summaries)
	entries, err := aq.prepare(db, q)
	if err != nil {
		return nil, err
	}
	err = db.RunQuery(q)
	if err != nil {
		return nil, err
	}
	return aq.buildResult(entries)
}

func (aq *AggregateQuery) prepare(db *DB, q *Query) (map[string]*AggregateEntry, error) {
	t := db.getTable(q.Table)
	if t == nil {
		return nil, fmt.Errorf("Table %v not found", q.Table)
	}

	dependencies := make(map[string]bool, len(aq.sortedFields)+len(aq.sortedSummaries))
	for _, field := range aq.sortedFields {
		for _, dependency := range field.DependsOn() {
			dependencies[dependency] = true
		}
	}
	for _, field := range aq.sortedSummaries {
		for _, dependency := range field.DependsOn() {
			dependencies[dependency] = true
		}
	}
	fields := make([]string, 0, len(dependencies))
	for dependency := range dependencies {
		fields = append(fields, dependency)
	}
	q.Fields = fields

	nativeResolution := t.resolution
	resolution := aq.Resolution
	if resolution == 0 {
		// Default to native resolution
		resolution = nativeResolution
	}
	if resolution < nativeResolution {
		return nil, fmt.Errorf("Aggregate query's resolution of %v is higher than table's native resolution of %v", resolution, nativeResolution)
	}
	if resolution%nativeResolution != 0 {
		return nil, fmt.Errorf("Aggregate query's resolution of %v is not evenly divisible by the table's native resolution of %v", resolution, nativeResolution)
	}
	scalingFactor := int(resolution / nativeResolution)
	// we'll calculate periods lazily later
	inPeriods := 0
	outPeriods := 0

	includedDims := make(map[string]bool, len(aq.Dims))
	for _, dim := range aq.Dims {
		includedDims[dim] = true
	}
	includeDim := func(dim string) bool {
		return includedDims[dim]
	}

	entries := make(map[string]*AggregateEntry, 0)
	q.OnValues = func(key map[string]interface{}, field string, vals []float64) {
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
			entry = &AggregateEntry{
				Dims:      key,
				Fields:    make(map[string][]expr.Accumulator, len(aq.Fields)+1),
				rawValues: make(map[string][][]float64),
			}
			if aq.Summaries != nil {
				entry.Summaries = make(map[string]expr.Accumulator, len(aq.Summaries))
				for _, summary := range aq.sortedSummaries {
					entry.Summaries[summary.Name] = summary.Accumulator()
				}
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

func (aq *AggregateQuery) buildResult(entries map[string]*AggregateEntry) ([]*AggregateEntry, error) {
	result := make([]*AggregateEntry, 0, len(entries))
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

			// Calculate per-period values
			for i := 0; i < entry.inPeriods; i++ {
				entry.inIdx = i
				entry.outIdx = i / entry.scalingFactor
				for j := 0; j < entry.numSamples; j++ {
					entry.valuesIdx = j
					vals[entry.outIdx].Update(entry)
				}
			}
		}

		// Also update summaries
		for _, summary := range aq.sortedSummaries {
			for i := 0; i < entry.inPeriods; i++ {
				entry.inIdx = i
				entry.outIdx = i / entry.scalingFactor
				for j := 0; j < entry.numSamples; j++ {
					entry.valuesIdx = j
					entry.Summaries[summary.Name].Update(entry)
				}
			}
		}

		result = append(result, entry)
	}

	if len(aq.OrderBy) > 0 {
		orderBy := make(map[string]bool, len(aq.OrderBy))
		for orderSummary, order := range aq.OrderBy {
			summaryFound := false
			for _, summary := range aq.sortedSummaries {
				if summary.Name == orderSummary {
					summaryFound = true
					break
				}
			}
			if !summaryFound {
				log.Errorf("Missing summary for %v from orderBy, ignoring", orderSummary)
				continue
			}
			orderBy[orderSummary] = order == ORDER_ASC
		}

		if len(orderBy) > 0 {
			sort.Sort(&orderedAggregated{result, orderBy})
		}
	}

	return result, nil
}

type orderedAggregated struct {
	r       []*AggregateEntry
	orderBy map[string]bool
}

func (r *orderedAggregated) Len() int      { return len(r.r) }
func (r *orderedAggregated) Swap(i, j int) { r.r[i], r.r[j] = r.r[j], r.r[i] }
func (r *orderedAggregated) Less(i, j int) bool {
	a := r.r[i]
	b := r.r[j]
	for summary, asc := range r.orderBy {
		fa := a.Summaries[summary].Get()
		fb := b.Summaries[summary].Get()
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
