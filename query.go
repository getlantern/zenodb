package zenodb

import (
	"fmt"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/sql"
)

type queryable interface {
	fields() []sql.Field
	resolution() time.Duration
	retentionPeriod() time.Duration
	truncateBefore() time.Time
	iterate(fields []string, onValue func(bytemap.ByteMap, []encoding.Sequence)) error
}

type query struct {
	fields      []string
	filter      goexpr.Expr
	asOf        time.Time
	asOfOffset  time.Duration
	until       time.Time
	untilOffset time.Duration
	onValues    func(key bytemap.ByteMap, field string, e expr.Expr, seq encoding.Sequence, startOffset int)
	t           queryable
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
	// Set up time-based parameters
	now := db.clock.Now()
	truncateBefore := q.t.truncateBefore()
	if q.asOf.IsZero() && q.asOfOffset >= 0 {
		log.Debug("No asOf and no positive asOfOffset, defaulting to retention period")
		q.asOf = truncateBefore
	}
	if q.asOf.IsZero() {
		q.asOf = now.Add(q.asOfOffset)
		log.Debugf("Defaulting asOf to %v based on now %v and asOfOffset %v", q.asOf, now, q.asOfOffset)
	}
	if q.asOf.Before(truncateBefore) {
		log.Debugf("asOf %v before end of retention window %v, using retention period instead", q.asOf.In(time.UTC), truncateBefore.In(time.UTC))
		q.asOf = truncateBefore
	}
	if q.until.IsZero() {
		q.until = now
		if q.untilOffset != 0 {
			q.until = q.until.Add(q.untilOffset)
		}
	}
	q.until = encoding.RoundTime(q.until, q.t.resolution())
	q.asOf = encoding.RoundTime(q.asOf, q.t.resolution())

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
	numPeriods := int(q.until.Sub(q.asOf) / q.t.resolution())
	log.Tracef("Query will return %d periods for range %v to %v", numPeriods, q.asOf, q.until)

	allFields := q.t.fields()
	iterateErr := q.t.iterate(q.fields, func(key bytemap.ByteMap, columns []encoding.Sequence) {
		log.Debugf("onValues %v %v : %v", allFields, key.AsMap(), columns)
		stats.Scanned++

		testedInclude := false
		shouldInclude := func() (bool, error) {
			return true, nil
		}
		if q.filter != nil {
			shouldInclude = func() (bool, error) {
				include := q.filter.Eval(key)
				inc, ok := include.(bool)
				if !ok {
					return false, fmt.Errorf("Filter expression returned something other than a boolean: %v", include)
				}
				if !inc {
					log.Debugf("WHERE rejected %v", key.AsMap())
					stats.FilterReject++
					return false, nil
				}
				stats.FilterPass++
				return true, nil
			}
		}

		log.Debugf("Num columns: %d", len(columns))
		for i := 0; i < len(columns); i++ {
			stats.ReadValue++
			field := allFields[i]
			e := field.Expr
			encodedWidth := e.EncodedWidth()
			seq := columns[i]
			log.Debugf("Sequence is: %v", seq.String(e))
			if len(seq) > 0 {
				stats.DataValid++
				if log.IsTraceEnabled() {
					log.Tracef("Reading encoding.Sequence %v", seq.String(e))
				}
				seq = seq.Truncate(encodedWidth, q.t.resolution(), q.asOf)
				log.Debugf("Truncated Sequence as of %v at resolution %v is: %v", q.asOf, q.t.resolution(), seq.String(e))
				if seq != nil {
					if !testedInclude {
						include, includeErr := shouldInclude()
						if includeErr != nil {
							log.Error(includeErr)
							return
						}
						if !include {
							return
						}
						testedInclude = true
					}
					stats.InTimeRange++
					startOffset := int(seq.Start().Sub(q.until) / q.t.resolution())
					q.onValues(key, field.Name, e, seq, startOffset)
				}
			}
		}
	})

	stats.Runtime = time.Now().Sub(start)
	return stats, iterateErr
}
