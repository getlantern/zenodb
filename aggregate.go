package tdb

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/oxtoacart/tdb/values"
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
	Summaries  []DerivedField
	OrderBy    map[string]Order
}

type AggregateEntry struct {
	Dims       map[string]interface{}
	Fields     map[string][]float64
	Summaries  map[string]float64
	NumPeriods int
	idx        int
}

// Get implements the method from interface govaluate.Parameters
func (entry *AggregateEntry) Get(name string) (interface{}, error) {
	vals := entry.Fields[name]
	if vals == nil {
		return entry.Summaries[name], nil
	}
	return vals[entry.idx], nil
}

func (aq *AggregateQuery) Run(db *DB, q *Query) ([]*AggregateEntry, error) {
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
				Dims:   key,
				Fields: make(map[string][]float64, len(aq.Fields)+1),
			}
			entries[ks] = entry
		}
		aggregatedVals := entry.Fields[field]
		if aggregatedVals == nil {
			if inBuckets < 1 {
				inBuckets = len(vals)
				// Limit inBuckets based on what we can fit into outBuckets
				inBuckets -= inBuckets % scalingFactor
				outBuckets = (inBuckets / scalingFactor) + 1
				inBuckets = outBuckets * scalingFactor
			}
			entry.NumPeriods = outBuckets
			aggregatedVals = make([]float64, outBuckets)
			entry.Fields[field] = aggregatedVals
		}
		for i := 0; i < len(vals); i++ {
			aggregatedVals[i/scalingFactor] += vals[i]
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
		for _, field := range aq.Fields {
			vals := make([]float64, entry.NumPeriods)
			for i := 0; i < entry.NumPeriods; i++ {
				entry.idx = i
				vals[i] = field.Expr(entry).Val()
			}
			entry.Fields[field.Name] = vals
		}

		if aq.Summaries != nil {
			entry.Summaries = make(map[string]float64, len(aq.Summaries))
			for _, summary := range aq.Summaries {
				var val values.Value
				for i := 0; i < entry.NumPeriods; i++ {
					entry.idx = i
					next := summary.Expr(entry)
					if i == 0 {
						val = next
					} else {
						val = val.Plus(next)
					}
				}
				log.Debugf("%v.%v %d : %v", entry.Dims["u"], entry.NumPeriods, summary.Name, val)
				entry.Summaries[summary.Name] = val.Val()
			}
		}

		result = append(result, entry)
	}

	if len(aq.OrderBy) > 0 {
		orderBy := make(map[string]bool, len(aq.OrderBy))
		for orderSummary, order := range aq.OrderBy {
			summaryFound := false
			for _, summary := range aq.Summaries {
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
		fa := a.Summaries[summary]
		fb := b.Summaries[summary]
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
