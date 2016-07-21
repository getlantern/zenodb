package tdb

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
	"github.com/tecbot/gorocksdb"
)

var (
	defaultReadOptions = gorocksdb.NewDefaultReadOptions()
)

func init() {
	// Don't bother filling the cache, since we're always doing full scans anyway
	defaultReadOptions.SetFillCache(false)
	// Make it a tailing iterator so that it doesn't create a snapshot (which
	// would have the effect of reducing the efficiency of applying merge ops
	// during compaction)
	defaultReadOptions.SetTailing(true)
}

type query struct {
	table         string
	fields        []string
	filter        *govaluate.EvaluableExpression
	asOf          time.Time
	asOfOffset    time.Duration
	until         time.Time
	untilOffset   time.Duration
	onValues      func(key bytemap.ByteMap, field string, vals []float64)
	t             *table
	encodedFields [][]byte
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

func (q *query) init(db *DB) error {
	if len(q.fields) == 0 {
		return fmt.Errorf("Please specify at least one field")
	}
	q.t = db.getTable(q.table)
	if q.t == nil {
		return fmt.Errorf("Unknown table %v", q.table)
	}

	// Sort fields lexicographically to match sorting in RocksDB so that we can
	// scan in order.
	q.encodedFields = make([][]byte, 0, len(q.fields))
	for _, field := range q.fields {
		q.encodedFields = append(q.encodedFields, encodeField(field))
	}
	sort.Sort(lexicographical(q.encodedFields))

	// Set up time-based parameters
	now := q.t.clock.Now()
	truncateBefore := q.t.truncateBefore()
	if q.asOf.IsZero() && q.asOfOffset >= 0 {
		log.Trace("No asOf and no positive asOfOffset, defaulting to retention period")
		q.asOf = truncateBefore
	}
	if q.asOf.IsZero() {
		q.asOf = now.Add(q.asOfOffset)
	}
	if q.asOf.Before(truncateBefore) {
		log.Tracef("asOf %v before end of retention window %v, using retention period instead", q.asOf.In(time.UTC), truncateBefore.In(time.UTC))
		q.asOf = truncateBefore
	}
	if q.until.IsZero() {
		q.until = now
		if q.untilOffset != 0 {
			q.until = q.until.Add(q.untilOffset)
		}
	}
	q.until = roundTime(q.until, q.t.Resolution)
	q.asOf = roundTime(q.asOf, q.t.Resolution)

	return nil
}

func (q *query) run(db *DB) (*QueryStats, error) {
	start := time.Now()
	stats := &QueryStats{}

	if q.t == nil {
		err := q.init(db)
		if err != nil {
			return nil, err
		}
	}
	numPeriods := int(q.until.Sub(q.asOf) / q.t.Resolution)
	log.Tracef("Query will return %d periods for range %v to %v", numPeriods, q.asOf, q.until)

	it := q.t.rdb.NewIterator(defaultReadOptions)
	defer it.Close()

	accums := q.t.getAccumulators()
	defer q.t.putAccumulators(accums)

	for it.SeekToFirst(); it.Valid(); it.Next() {
		stats.Scanned++
		k := it.Key()
		key, field := keyAndFieldFor(k.Data())

		var e expr.Expr
		var accum expr.Accumulator
		for i, candidate := range q.t.Fields {
			if candidate.Name == field {
				e = q.t.Fields[i]
				accum = accums[i]
				break
			}
		}
		if e == nil {
			return stats, fmt.Errorf("Unknown field %v", field)
		}
		encodedWidth := accum.EncodedWidth()

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
				startOffset := int(seqStart.Sub(to) / q.t.Resolution)
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
