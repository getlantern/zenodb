package tdb

import (
	"time"

	"github.com/getlantern/bytemap"
	. "github.com/getlantern/tdb/expr"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSequenceUpdate(t *testing.T) {
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
			checkUpdatedValues(t, e.Accumulator(), seq, trunc(expected, 4))
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

func checkUpdatedValues(t *testing.T, accum Accumulator, seq sequence, expected []float64) {
	if assert.Equal(t, len(expected), seq.numPeriods(accum.EncodedWidth())) {
		for i, v := range expected {
			assert.EqualValues(t, v, seq.valueAt(i, accum))
		}
	}
}

func TestSequenceMergeAOB(t *testing.T) {
	res := time.Minute
	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	truncateBefore := epoch.Add(-1000 * res)
	e := SUM("a")
	accum1 := e.Accumulator()
	accum2 := e.Accumulator()

	var seq1 sequence
	var seq2 sequence

	seq1 = seq1.update(newTSParams(epoch.Add(-1*res), bytemap.NewFloat(map[string]float64{"a": 1})), accum1, res, truncateBefore)
	seq1 = seq1.update(newTSParams(epoch.Add(-3*res), bytemap.NewFloat(map[string]float64{"a": 3})), accum1, res, truncateBefore)

	seq2 = seq2.update(newTSParams(epoch.Add(-3*res), bytemap.NewFloat(map[string]float64{"a": 3})), accum1, res, truncateBefore)
	seq2 = seq2.update(newTSParams(epoch.Add(-4*res), bytemap.NewFloat(map[string]float64{"a": 4})), accum1, res, truncateBefore)
	seq2 = seq2.update(newTSParams(epoch.Add(-5*res), bytemap.NewFloat(map[string]float64{"a": 5})), accum1, res, truncateBefore)

	checkMerge(t, epoch, res, seq1, seq2, accum1, accum2)
	checkMerge(t, epoch, res, seq2, seq1, accum1, accum2)
}

func TestSequenceMergeAOA(t *testing.T) {
	res := time.Minute
	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	truncateBefore := epoch.Add(-1000 * res)
	e := SUM("a")
	accum1 := e.Accumulator()
	accum2 := e.Accumulator()

	var seq1 sequence
	var seq2 sequence

	seq1 = seq1.update(newTSParams(epoch.Add(-1*res), bytemap.NewFloat(map[string]float64{"a": 1})), accum1, res, truncateBefore)
	seq1 = seq1.update(newTSParams(epoch.Add(-3*res), bytemap.NewFloat(map[string]float64{"a": 3})), accum1, res, truncateBefore)
	seq1 = seq1.update(newTSParams(epoch.Add(-4*res), bytemap.NewFloat(map[string]float64{"a": 4})), accum1, res, truncateBefore)
	seq1 = seq1.update(newTSParams(epoch.Add(-5*res), bytemap.NewFloat(map[string]float64{"a": 5})), accum1, res, truncateBefore)

	seq2 = seq2.update(newTSParams(epoch.Add(-3*res), bytemap.NewFloat(map[string]float64{"a": 3})), accum1, res, truncateBefore)

	checkMerge(t, epoch, res, seq1, seq2, accum1, accum2)
	checkMerge(t, epoch, res, seq2, seq1, accum1, accum2)
}

func TestSequenceMergeAB(t *testing.T) {
	res := time.Minute
	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	truncateBefore := epoch.Add(-1000 * res)
	e := SUM("a")
	accum1 := e.Accumulator()
	accum2 := e.Accumulator()

	var seq1 sequence
	var seq2 sequence

	seq1 = seq1.update(newTSParams(epoch.Add(-1*res), bytemap.NewFloat(map[string]float64{"a": 1})), accum1, res, truncateBefore)
	seq2 = seq2.update(newTSParams(epoch.Add(-3*res), bytemap.NewFloat(map[string]float64{"a": 6})), accum1, res, truncateBefore)
	seq2 = seq2.update(newTSParams(epoch.Add(-4*res), bytemap.NewFloat(map[string]float64{"a": 4})), accum1, res, truncateBefore)
	seq2 = seq2.update(newTSParams(epoch.Add(-5*res), bytemap.NewFloat(map[string]float64{"a": 5})), accum1, res, truncateBefore)

	checkMerge(t, epoch, res, seq1, seq2, accum1, accum2)
	checkMerge(t, epoch, res, seq2, seq1, accum1, accum2)
}

func checkMerge(t *testing.T, epoch time.Time, res time.Duration, seq1 sequence, seq2 sequence, accum1 Accumulator, accum2 Accumulator) {
	merged := seq1.merge(seq2, res, accum1, accum2)
	assert.Equal(t, 5, merged.numPeriods(accum1.EncodedWidth()))
	assert.EqualValues(t, 1, merged.valueAtTime(epoch.Add(-1*res), accum1, res))
	assert.EqualValues(t, 0, merged.valueAtTime(epoch.Add(-2*res), accum1, res))
	assert.EqualValues(t, 6, merged.valueAtTime(epoch.Add(-3*res), accum1, res))
	assert.EqualValues(t, 4, merged.valueAtTime(epoch.Add(-4*res), accum1, res))
	assert.EqualValues(t, 5, merged.valueAtTime(epoch.Add(-5*res), accum1, res))
}
