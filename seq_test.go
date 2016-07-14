package tdb

import (
	"time"

	"github.com/getlantern/bytemap"
	. "github.com/getlantern/tdb/expr"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSequence(t *testing.T) {
	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	res := time.Minute
	e := SUM(MULT(FIELD("a"), FIELD("b")))

	checkWithTruncation := func(retainPeriods int) {
		t.Logf("Retention periods: %d", retainPeriods)
		retentionPeriod := res * time.Duration(retainPeriods)
		trunc := func(vals []float64, ignoreTrailingIndex int) []float64 {
			if len(vals) > retainPeriods {
				vals = vals[:retainPeriods]
				if len(vals)-1 == ignoreTrailingIndex {
					// Remove trailing zero to deal with append deep
					vals = vals[:retainPeriods-1]
				}
			}
			return vals
		}

		start := epoch
		var seq sequence

		doIt := func(ts time.Time, params map[string]float64, expected []float64) {
			if ts.After(start) {
				start = ts
			}
			truncateBefore := start.Add(-1 * retentionPeriod)
			seq = seq.update(newTSParams(ts, bytemap.NewFloat(params)), e.Accumulator(), res, truncateBefore)
			checkValues(t, e.Accumulator(), seq, trunc(expected, 4))
		}

		// Set something on an empty sequence
		doIt(epoch, map[string]float64{"a": 1, "b": 2}, []float64{2})

		// Prepend
		doIt(epoch.Add(2*res), map[string]float64{"a": 1, "b": 1}, []float64{1, 0, 2})

		// Append
		doIt(epoch.Add(-1*res), map[string]float64{"a": 1, "b": 3}, []float64{1, 0, 2, 3})

		// Append deep
		doIt(epoch.Add(-3*res), map[string]float64{"a": 1, "b": 4}, []float64{1, 0, 2, 3, 0, 4})

		// Update value
		doIt(epoch, map[string]float64{"a": 1, "b": 5}, []float64{1, 0, 7, 3, 0, 4})
	}

	for i := 6; i >= 0; i-- {
		checkWithTruncation(i)
	}
}

func checkValues(t *testing.T, accum Accumulator, seq sequence, expected []float64) {
	if assert.Equal(t, len(expected), seq.numPeriods(accum.EncodedWidth())) {
		for i, v := range expected {
			assert.EqualValues(t, v, seq.valueAt(i, accum))
		}
	}
}
