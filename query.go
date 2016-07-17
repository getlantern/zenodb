package tdb

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
)

type query struct {
	table       string
	fields      []string
	filter      *govaluate.EvaluableExpression
	asOf        time.Time
	asOfOffset  time.Duration
	until       time.Time
	untilOffset time.Duration
	onValues    func(key bytemap.ByteMap, field string, vals []float64)
}

type QueryStats struct {
	Scanned      int64
	FilterPass   int64
	FilterReject int64
	ReadValue    int64
	DataValid    int64
	InTimeRange  int64
	Runtime      time.Duration
}

func (db *DB) runQuery(q *query) (*QueryStats, error) {
	start := time.Now()
	stats := &QueryStats{}

	if q.asOf.IsZero() && q.asOfOffset >= 0 {
		return stats, fmt.Errorf("Please specify an asOf or a negative asOfOffset")
	}
	if len(q.fields) == 0 {
		return stats, fmt.Errorf("Please specify at least one field")
	}
	t := db.getTable(q.table)
	if t == nil {
		return stats, fmt.Errorf("Unknown table %v", q.table)
	}

	// Sort fields lexicographically to match sorting in RocksDB so that we can
	// scan in order.
	fields := make([][]byte, 0, len(q.fields))
	for _, field := range q.fields {
		fields = append(fields, encodeField(field))
	}
	sort.Sort(lexicographical(fields))

	// Set up time-based parameters
	now := t.clock.Now()
	if q.until.IsZero() {
		q.until = now
		if q.untilOffset != 0 {
			q.until = q.until.Add(q.untilOffset)
		}
	}
	if q.asOf.IsZero() {
		q.asOf = now.Add(q.asOfOffset)
	}
	q.until = roundTime(q.until, t.Resolution)
	q.asOf = roundTime(q.asOf, t.Resolution)
	numPeriods := int(q.until.Sub(q.asOf) / t.Resolution)
	log.Tracef("Query will return %d periods for range %v to %v", numPeriods, q.asOf, q.until)

	it := t.rdb.NewIterator(defaultReadOptions)
	defer it.Close()

	accums := t.getAccumulators()
	defer t.putAccumulators(accums)

	for i, fieldBytes := range fields {
		field := q.fields[i]
		var e expr.Expr
		var accum expr.Accumulator
		for i, candidate := range t.Fields {
			if candidate.Name == field {
				e = t.Fields[i]
				accum = accums[i]
				break
			}
		}
		encodedWidth := accum.EncodedWidth()

		for it.Seek(fieldBytes); it.ValidForPrefix(fieldBytes); it.Next() {
			stats.Scanned++
			k := it.Key()
			key := keyFor(k.Data())

			if q.filter != nil {
				include, err := q.filter.Eval(bytemapQueryParams(key))
				if err != nil {
					k.Free()
					return stats, fmt.Errorf("Unable to apply filter: %v", err)
				}
				inc, ok := include.(bool)
				if !ok {
					k.Free()
					return stats, fmt.Errorf("Filter expression returned something other than a boolean: %v", include)
				}
				if !inc {
					stats.FilterReject++
					continue
				}
				stats.FilterPass++
			}

			v := it.Value()
			stats.ReadValue++
			seq := sequence(v.Data())
			vals := make([]float64, numPeriods)
			if len(seq) > 0 {
				stats.DataValid++
				seqStart := seq.start()
				copyPeriods := seq.numPeriods(encodedWidth)
				if log.IsTraceEnabled() {
					log.Tracef("Reading sequence %v", seq.String(e))
				}
				includeKey := false
				if !seqStart.Before(q.asOf) {
					to := q.until
					if to.After(seqStart) {
						to = seqStart
					}
					startOffset := int(seqStart.Sub(to) / t.Resolution)
					log.Tracef("Start offset %d", startOffset)
					for i := 0; i+startOffset < copyPeriods && i < numPeriods; i++ {
						includeKey = true
						val := seq.valueAt(i+startOffset, accum)
						log.Tracef("Grabbing value %f", val)
						vals[i] = val
					}
				}
				if includeKey {
					stats.InTimeRange++
					q.onValues(key, field, vals)
				}
			}
			k.Free()
			v.Free()
		}
	}

	stats.Runtime = time.Now().Sub(start)
	return stats, nil
}

type lexicographical [][]byte

func (a lexicographical) Len() int           { return len(a) }
func (a lexicographical) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a lexicographical) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) < 0 }

func keysEqual(a map[string]interface{}, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
