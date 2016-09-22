package encoding

import (
	"math/rand"
	"time"

	"github.com/getlantern/bytemap"
	. "github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	res            = time.Minute
	epoch          = time.Date(2015, 5, 6, 7, 8, 0, 0, time.UTC)
	truncateBefore = epoch.Add(-1000 * res)
)

func TestSequenceUpdate(t *testing.T) {
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
		var seq Sequence

		doIt := func(ts time.Time, params map[string]float64, expected []float64) {
			if ts.After(start) {
				start = ts
			}
			tb := start.Add(-1 * retentionPeriod)
			seq = seq.Update(NewTSParams(ts, bytemap.NewFloat(params)), nil, e, res, tb)
			checkUpdatedValues(t, e, seq, trunc(expected, 4))
		}

		// Set something on an empty Sequence
		doIt(epoch, map[string]float64{"a": 1, "b": 2}, []float64{2})

		// Prepend
		doIt(epoch.Add(2*res).Add(randBelow(res)), map[string]float64{"a": 1, "b": 1}, []float64{1, 0, 2})

		// Append
		doIt(epoch.Add(-1*res).Add(randBelow(res)), map[string]float64{"a": 1, "b": 3}, []float64{1, 0, 2, 3})

		// Append deep
		doIt(epoch.Add(-3*res).Add(randBelow(res)), map[string]float64{"a": 1, "b": 4}, []float64{1, 0, 2, 3, 0, 4})

		// Update value
		doIt(epoch.Add(randBelow(res)), map[string]float64{"a": 1, "b": 5}, []float64{1, 0, 7, 3, 0, 4})
	}

	for i := 6; i >= 0; i-- {
		checkWithTruncation(i)
	}
}

func checkUpdatedValues(t *testing.T, e Expr, seq Sequence, expected []float64) {
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
	e := SUM("a")

	var seq1 Sequence
	var seq2 Sequence

	seq1 = seq1.Update(NewTSParams(epoch.Add(-1*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 1})), nil, e, res, truncateBefore)
	seq1 = seq1.Update(NewTSParams(epoch.Add(-3*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 3})), nil, e, res, truncateBefore)

	seq2 = seq2.Update(NewTSParams(epoch.Add(-3*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 3})), nil, e, res, truncateBefore)
	seq2 = seq2.Update(NewTSParams(epoch.Add(-4*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 4})), nil, e, res, truncateBefore)
	seq2 = seq2.Update(NewTSParams(epoch.Add(-5*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 5})), nil, e, res, truncateBefore)

	checkMerge(t, epoch, res, seq1, seq2, e)
	checkMerge(t, epoch, res, seq2, seq1, e)
}

func TestSequenceMergeAOA(t *testing.T) {
	e := SUM("a")

	var seq1 Sequence
	var seq2 Sequence

	seq1 = seq1.Update(NewTSParams(epoch.Add(-1*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 1})), nil, e, res, truncateBefore)
	seq1 = seq1.Update(NewTSParams(epoch.Add(-3*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 3})), nil, e, res, truncateBefore)
	seq1 = seq1.Update(NewTSParams(epoch.Add(-4*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 4})), nil, e, res, truncateBefore)
	seq1 = seq1.Update(NewTSParams(epoch.Add(-5*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 5})), nil, e, res, truncateBefore)

	seq2 = seq2.Update(NewTSParams(epoch.Add(-3*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 3})), nil, e, res, truncateBefore)

	checkMerge(t, epoch, res, seq1, seq2, e)
	checkMerge(t, epoch, res, seq2, seq1, e)
}

func TestSequenceMergeAB(t *testing.T) {
	e := SUM("a")

	var seq1 Sequence
	var seq2 Sequence

	seq1 = seq1.Update(NewTSParams(epoch.Add(-1*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 1})), nil, e, res, truncateBefore)
	seq2 = seq2.Update(NewTSParams(epoch.Add(-3*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 6})), nil, e, res, truncateBefore)
	seq2 = seq2.Update(NewTSParams(epoch.Add(-4*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 4})), nil, e, res, truncateBefore)
	seq2 = seq2.Update(NewTSParams(epoch.Add(-5*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 5})), nil, e, res, truncateBefore)
	seq2 = seq2.Merge(nil, e, res, zeroTime)
	seq2 = ((Sequence)(nil)).Merge(seq2, e, res, zeroTime)

	checkMerge(t, epoch, res, seq1, seq2, e)
	checkMerge(t, epoch, res, seq2, seq1, e)
}

func checkMerge(t *testing.T, epoch time.Time, res time.Duration, seq1 Sequence, seq2 Sequence, e Expr) {
	merged := seq1.Merge(seq2, e, res, zeroTime)
	assert.Equal(t, 5, merged.NumPeriods(e.EncodedWidth()))
	val, _ := merged.ValueAtTime(epoch.Add(-1*res).Add(randBelow(res)), e, res)
	assert.EqualValues(t, 1, val)
	val, _ = merged.ValueAtTime(epoch.Add(-2*res).Add(randBelow(res)), e, res)
	assert.EqualValues(t, 0, val)
	val, _ = merged.ValueAtTime(epoch.Add(-3*res).Add(randBelow(res)), e, res)
	assert.EqualValues(t, 6, val)
	val, _ = merged.ValueAtTime(epoch.Add(-4*res).Add(randBelow(res)), e, res)
	assert.EqualValues(t, 4, val)
	val, _ = merged.ValueAtTime(epoch.Add(-5*res).Add(randBelow(res)), e, res)
	assert.EqualValues(t, 5, val)
}

func TestSequenceMergeValueAt(t *testing.T) {
	e := SUM("a")

	var seq1 Sequence
	var seq2 Sequence

	seq1 = seq1.Update(NewTSParams(epoch.Add(-1*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 1})), nil, e, res, truncateBefore)
	seq1 = seq1.Update(NewTSParams(epoch.Add(-3*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 3})), nil, e, res, truncateBefore)

	seq2 = seq2.Update(NewTSParams(epoch.Add(-1*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 1})), nil, e, res, truncateBefore)
	seq2 = seq2.Update(NewTSParams(epoch.Add(-2*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 2})), nil, e, res, truncateBefore)
	seq2 = seq2.Update(NewTSParams(epoch.Add(-3*res).Add(randBelow(res)), bytemap.NewFloat(map[string]float64{"a": 3})), nil, e, res, truncateBefore)

	for i := 0; i < 3; i++ {
		other, found := seq2.DataAt(i, e)
		if !assert.True(t, found) {
			return
		}
		seq1.MergeValueAt(i, e, other)
	}

	for i, expected := range []float64{2, 2, 6} {
		val, found := seq1.ValueAt(i, e)
		if !assert.True(t, found) {
			return
		}
		assert.EqualValues(t, expected, val)
	}
}

func TestSequenceTruncate(t *testing.T) {
	res := 1 * time.Hour
	e := SUM("a")
	seq := NewSequence(e.EncodedWidth(), 10)
	seq.SetStart(epoch)
	assert.Nil(t, seq.Truncate(e.EncodedWidth(), res, epoch))
	assert.Nil(t, seq.Truncate(e.EncodedWidth(), res, epoch.Add(-1*res)))
	assert.Equal(t, 1, seq.Truncate(e.EncodedWidth(), res, epoch.Add(-2*res)).NumPeriods(e.EncodedWidth()))
}

func randBelow(res time.Duration) time.Duration {
	return time.Duration(-1 * rand.Intn(int(res)))
}
