package tdb

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/bytemap"
)

type query struct {
	table        string
	fields       []string
	filter       *govaluate.EvaluableExpression
	asOf         time.Time
	asOfOffset   time.Duration
	until        time.Time
	untilOffset  time.Duration
	onValues     func(key bytemap.ByteMap, field string, vals []float64)
	t            *table
	sortedFields []string
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

	accums := q.t.getAccumulators()
	defer q.t.putAccumulators(accums)

	for i, field := range q.t.Fields {
		includedInQuery := false
		for _, f := range q.fields {
			if f == field.Name {
				includedInQuery = true
				break
			}
		}
		if !includedInQuery {
			continue
		}

		e := field.Expr
		accum := field.Accumulator()
		encodedWidth := accum.EncodedWidth()

		q.t.columnStores[i].iterate(func(key bytemap.ByteMap, seq sequence) {
			stats.Scanned++

			if q.filter != nil {
				include, err := q.filter.Eval(bytemapQueryParams(key))
				if err != nil {
					log.Errorf("Unable to apply filter: %v", err)
					return
				}
				inc, ok := include.(bool)
				if !ok {
					log.Errorf("Filter expression returned something other than a boolean: %v", include)
					return
				}
				if !inc {
					stats.FilterReject++
					return
				}
				stats.FilterPass++
			}

			stats.ReadValue++
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
					q.onValues(key, field.Name, vals)
				}
			}
		})
	}

	stats.Runtime = time.Now().Sub(start)
	return stats, nil
}
