package sequence

import (
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/bmp"
	"github.com/getlantern/tdb/enc"
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
		var seq Seq

		doIt := func(ts time.Time, params map[string]float64, expected []float64) {
			if ts.After(start) {
				start = ts
			}
			truncateBefore := start.Add(-1 * retentionPeriod)
			seq = seq.Update(ts, bmp.Params(bytemap.NewFloat(params)), e, res, truncateBefore)
			checkUpdatedValues(t, e, seq, trunc(expected, 4))
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

func checkUpdatedValues(t *testing.T, e Expr, seq Seq, expected []float64) {
	if assert.Equal(t, len(expected), seq.NumPeriods(e.EncodedWidth())) {
		for i, v := range expected {
			actual, wasSet := seq.ValueAt(i, e)
			assert.EqualValues(t, v, actual)
			if v == 0 {
				assert.False(t, wasSet)
			}
		}
	}
}

func TestSequenceMergeAOB(t *testing.T) {
	res := time.Minute
	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	truncateBefore := epoch.Add(-1000 * res)
	e := SUM("a")

	var seq1 Seq
	var seq2 Seq

	seq1 = seq1.Update(epoch.Add(-1*res), bmparams(1), e, res, truncateBefore)
	seq1 = seq1.Update(epoch.Add(-3*res), bmparams(3), e, res, truncateBefore)

	seq2 = seq2.Update(epoch.Add(-3*res), bmparams(3), e, res, truncateBefore)
	seq2 = seq2.Update(epoch.Add(-4*res), bmparams(4), e, res, truncateBefore)
	seq2 = seq2.Update(epoch.Add(-5*res), bmparams(5), e, res, truncateBefore)

	checkMerge(t, epoch, res, seq1, seq2, e)
	checkMerge(t, epoch, res, seq2, seq1, e)
}

func TestSequenceMergeAOA(t *testing.T) {
	res := time.Minute
	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	truncateBefore := epoch.Add(-1000 * res)
	e := SUM("a")

	var seq1 Seq
	var seq2 Seq

	seq1 = seq1.Update(epoch.Add(-1*res), bmparams(1), e, res, truncateBefore)
	seq1 = seq1.Update(epoch.Add(-3*res), bmparams(3), e, res, truncateBefore)
	seq1 = seq1.Update(epoch.Add(-4*res), bmparams(4), e, res, truncateBefore)
	seq1 = seq1.Update(epoch.Add(-5*res), bmparams(5), e, res, truncateBefore)

	seq2 = seq2.Update(epoch.Add(-3*res), bmparams(3), e, res, truncateBefore)

	checkMerge(t, epoch, res, seq1, seq2, e)
	checkMerge(t, epoch, res, seq2, seq1, e)
}

func TestSequenceMergeAB(t *testing.T) {
	res := time.Minute
	epoch := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	truncateBefore := epoch.Add(-1000 * res)
	e := SUM("a")

	var seq1 Seq
	var seq2 Seq

	seq1 = seq1.Update(epoch.Add(-1*res), bmparams(1), e, res, truncateBefore)
	seq2 = seq2.Update(epoch.Add(-3*res), bmparams(6), e, res, truncateBefore)
	seq2 = seq2.Update(epoch.Add(-4*res), bmparams(4), e, res, truncateBefore)
	seq2 = seq2.Update(epoch.Add(-5*res), bmparams(5), e, res, truncateBefore)
	seq2 = seq2.Merge(nil, e, res, enc.ZeroTime)
	seq2 = ((Seq)(nil)).Merge(seq2, e, res, enc.ZeroTime)

	checkMerge(t, epoch, res, seq1, seq2, e)
	checkMerge(t, epoch, res, seq2, seq1, e)
}

func checkMerge(t *testing.T, epoch time.Time, res time.Duration, seq1 Seq, seq2 Seq, e Expr) {
	merged := seq1.Merge(seq2, e, res, enc.ZeroTime)
	assert.Equal(t, 5, merged.NumPeriods(e.EncodedWidth()))
	val, _ := merged.ValueAtTime(epoch.Add(-1*res), e, res)
	assert.EqualValues(t, 1, val)
	val, _ = merged.ValueAtTime(epoch.Add(-2*res), e, res)
	assert.EqualValues(t, 0, val)
	val, _ = merged.ValueAtTime(epoch.Add(-3*res), e, res)
	assert.EqualValues(t, 6, val)
	val, _ = merged.ValueAtTime(epoch.Add(-4*res), e, res)
	assert.EqualValues(t, 4, val)
	val, _ = merged.ValueAtTime(epoch.Add(-5*res), e, res)
	assert.EqualValues(t, 5, val)
}

func bmparams(val float64) Params {
	return bmp.Params(bytemap.NewFloat(map[string]float64{"a": val}))
}
