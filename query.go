package zenodb

import (
	"fmt"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
)

type query struct {
	table       string
	fields      []string
	filter      goexpr.Expr
	asOf        time.Time
	asOfOffset  time.Duration
	until       time.Time
	untilOffset time.Duration
	onValues    func(key bytemap.ByteMap, field string, e expr.Expr, seq encoding.Sequence, startOffset int)
	t           *table
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
	q.t = db.getTable(q.table)
	if q.t == nil {
		return fmt.Errorf("Unknown table %v", q.table)
	}

	// Set up time-based parameters
	now := db.clock.Now()
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
	q.until = encoding.RoundTime(q.until, q.t.Resolution)
	q.asOf = encoding.RoundTime(q.asOf, q.t.Resolution)

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

	q.t.rowStore.iterate(q.fields, func(key bytemap.ByteMap, columns []encoding.Sequence) {
		stats.Scanned++

		if q.filter != nil {
			include := q.filter.Eval(key)
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

		for i := 0; i < len(columns); i++ {
			stats.ReadValue++
			field := q.t.Fields[i]
			e := field.Expr
			encodedWidth := e.EncodedWidth()
			seq := columns[i]
			if len(seq) > 0 {
				stats.DataValid++
				if log.IsTraceEnabled() {
					log.Tracef("Reading encoding.Sequence %v", seq.String(e))
				}
				seq = seq.Truncate(encodedWidth, q.t.Resolution, q.asOf)
				if seq != nil {
					stats.InTimeRange++
					startOffset := int(seq.Start().Sub(q.until) / q.t.Resolution)
					q.onValues(key, field.Name, e, seq, startOffset)
				}
			}
		}
	})

	stats.Runtime = time.Now().Sub(start)
	return stats, nil
}
