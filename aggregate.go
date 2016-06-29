package tdb

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

const (
	ORDER_ASC  = 1
	ORDER_DESC = 2
)

type Order int

type AggregateQuery struct {
	Resolution time.Duration
	Dims       []string
	Fields     []DerivedField
	OrderBy    map[string]Order
}

type AggregateResult struct {
	Dims       []interface{}
	Fields     [][]float64
	NumPeriods int
}

type aggregateEntry struct {
	key    map[string]interface{}
	fields map[string][]float64
	idx    int
}

// Get implements the method from interface govaluate.Parameters
func (entry *aggregateEntry) Get(name string) (interface{}, error) {
	vals := entry.fields[name]
	if vals == nil {
		return float64(0), nil
	}
	return vals[entry.idx], nil
}

func (aq *AggregateQuery) Run(db *DB, q *Query) ([]*AggregateResult, error) {
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

func (aq *AggregateQuery) prepare(db *DB, q *Query) (map[string]*aggregateEntry, error) {
	t := db.getTable(q.Table)
	if t == nil {
		return nil, fmt.Errorf("Table %v not found", q.Table)
	}
	nativeResolution := t.resolution
	if aq.Resolution < nativeResolution {
		return nil, fmt.Errorf("Aggregate query's resolution of %v is higher than table's native resolution of %v", aq.Resolution, nativeResolution)
	}
	if aq.Resolution%nativeResolution != 0 {
		return nil, fmt.Errorf("Aggregate query's resolution of %v is not evenly divisible by the table's native resolution of %v", aq.Resolution, nativeResolution)
	}
	scalingFactor := int(aq.Resolution / nativeResolution)
	// we'll calculate buckets lazily later
	inBuckets := 0
	outBuckets := 0

	includedDims := make(map[string]bool, len(aq.Dims))
	for _, dim := range aq.Dims {
		includedDims[dim] = true
	}
	includeDim := func(dim string) bool {
		return includedDims[dim]
	}

	entries := make(map[string]*aggregateEntry, 0)
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
			entry = &aggregateEntry{
				key:    key,
				fields: make(map[string][]float64, 1),
			}
			entries[ks] = entry
		}
		aggregatedVals := entry.fields[field]
		if aggregatedVals == nil {
			if inBuckets < 1 {
				inBuckets = len(vals)
				// Limit inBuckets based on what we can fit into outBuckets
				inBuckets -= inBuckets % scalingFactor
				outBuckets = (inBuckets / scalingFactor) + 1
				inBuckets = outBuckets * scalingFactor
			}
			aggregatedVals = make([]float64, outBuckets)
			entry.fields[field] = aggregatedVals
		}
		for i := 0; i < len(vals); i++ {
			aggregatedVals[i/scalingFactor] += vals[i]
		}
	}

	return entries, nil
}

func (aq *AggregateQuery) buildResult(entries map[string]*aggregateEntry) ([]*AggregateResult, error) {
	dimIndexes := make(map[string]int, len(aq.Dims))
	for i, dim := range aq.Dims {
		dimIndexes[dim] = i
	}
	result := make([]*AggregateResult, 0, len(entries))

	if len(entries) == 0 {
		return result, nil
	}

	for _, entry := range entries {
		numBuckets := 0
		for _, fieldValues := range entry.fields {
			numBuckets = len(fieldValues)
			break
		}

		r := &AggregateResult{
			Dims:       make([]interface{}, len(aq.Dims)),
			Fields:     make([][]float64, len(aq.Fields)),
			NumPeriods: numBuckets,
		}
		for i, dim := range aq.Dims {
			r.Dims[i] = entry.key[dim]
		}

		for fi, field := range aq.Fields {
			vals := make([]float64, numBuckets)
			for i := 0; i < numBuckets; i++ {
				entry.idx = i
				vals[i] = field.Expr(entry).Val()
			}
			r.Fields[fi] = vals
		}

		result = append(result, r)

		if len(aq.OrderBy) > 0 {
			fieldIndexes := make(map[string]int, len(aq.Fields))
			for i, field := range aq.Fields {
				fieldIndexes[field.Name] = i
			}

			orderBy := make(map[int]bool, len(aq.OrderBy))
			for field, order := range aq.OrderBy {
				idx, ok := fieldIndexes[field]
				if !ok {
					log.Errorf("Field %v from orderByFields not selected, ignoring", field)
					continue
				}
				orderBy[idx] = order == ORDER_ASC
			}

			if len(orderBy) > 0 {
				sort.Sort(&orderedAggregateResults{result, orderBy})
			}
		}
	}

	return result, nil
}

type orderedAggregateResults struct {
	r       []*AggregateResult
	orderBy map[int]bool
}

func (r *orderedAggregateResults) Len() int      { return len(r.r) }
func (r *orderedAggregateResults) Swap(i, j int) { r.r[i], r.r[j] = r.r[j], r.r[i] }
func (r *orderedAggregateResults) Less(i, j int) bool {
	a := r.r[i]
	b := r.r[j]
	for field, asc := range r.orderBy {
		fa := a.Fields[field][0]
		fb := b.Fields[field][0]
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

func keyToBytes(key map[string]interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := msgpack.NewEncoder(buf)
	enc.SortMapKeys(true)
	err := enc.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("Unable to encode dims: %v", err)
	}
	return buf.Bytes(), nil
}

func keyFromBytes(keyBytes []byte) (map[string]interface{}, error) {
	key := make(map[string]interface{}, 0)
	err := msgpack.Unmarshal(keyBytes, &key)
	if err != nil {
		return nil, fmt.Errorf("Unable to decode dims: %v", err)
	}
	return key, nil
}
