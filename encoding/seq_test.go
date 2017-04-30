package encoding

import (
	"github.com/getlantern/bytemap"
	. "github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

var (
	res            = time.Minute
	epoch          = time.Date(2015, 1, 1, 1, 2, 0, 0, time.UTC)
	truncateBefore = epoch.Add(-1000 * res)
)

func TestSequenceOnly(t *testing.T) {
	length := 5
	resolution := 11 * time.Minute
	start := epoch
	until := start.Add(resolution * time.Duration(length))
	e := SUM(MULT(FIELD("a"), FIELD("b")))
	seq := NewSequence(e.EncodedWidth(), length)
	seq.SetUntil(until)
	for i := 0; i < length; i++ {
		delta := time.Duration(i+1)*resolution + randBelow(resolution)
		ts := start.Add(delta)
		val := float64(i + 1)
		seq.UpdateValue(ts, bytemapParams(bytemap.NewFloat(map[string]float64{"a": 1, "b": val})), nil, e, resolution, start)
		valAtTime, found := seq.ValueAtTime(ts, e, resolution)
		if assert.True(t, found) {
			assert.Equal(t, val, valAtTime)
		}
	}
	for i := 0; i < length; i++ {
		val, found := seq.ValueAt(i, e)
		if assert.True(t, found, "No value found for %d", i) {
			assert.Equal(t, float64(length-i), val, "Wrong value found for %d", i)
		}
	}
	for j := 0; j < length; j++ {
		delta := time.Duration(j)*resolution + randBelow(resolution)
		ts := start.Add(delta)
		seq = seq.Truncate(e.EncodedWidth(), resolution, ts, time.Time{})
		for i := 0; i < length-j; i++ {
			val, found := seq.ValueAt(i, e)
			if assert.True(t, found, "No value found for %d on truncating %d", i, j) {
				assert.Equal(t, float64(length-i), val, "Wrong value found for %d", i)
			}
		}
	}
}

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

func TestSequenceFull(t *testing.T) {
	resolutionIn := res
	resolutionOut := 3 * resolutionIn

	eOut := ADD(SUM(FIELD("a")), SUM(FIELD("b")))
	eIn := SUM(FIELD("a"))
	eB := SUM(FIELD("b"))
	submergers := eOut.SubMergers([]Expr{eIn, eB})

	inPeriods := int(10 * resolutionOut / resolutionIn)
	widthOut := eOut.EncodedWidth()
	widthIn := eIn.EncodedWidth()
	seqIn := NewSequence(widthIn, inPeriods)

	params := FloatParams(1)
	for i := 0; i < inPeriods; i++ {
		seqIn.UpdateValueAt(i, eIn, params, nil)
	}

	asOf := epoch.Add(-1 * time.Duration(inPeriods) * resolutionIn)
	until := epoch

	for _, seqOut := range []Sequence{NewSequence(widthOut, 5), nil} {
		if seqOut != nil {
			seqOut.SetUntil(epoch.Add(-2 * resolutionOut))
			assert.Equal(t, epoch.Add(-2*resolutionOut).In(time.UTC), seqOut.Until().In(time.UTC))
			assert.Equal(t, epoch.Add(-7*resolutionOut).In(time.UTC), seqOut.AsOf(widthOut, resolutionOut).In(time.UTC))
		}

		seqIn.SetUntil(epoch)
		assert.Equal(t, epoch, seqIn.Until().In(time.UTC))
		assert.Equal(t, epoch.Add(-1*time.Duration(inPeriods)*resolutionIn).In(time.UTC), seqIn.AsOf(widthIn, resolutionIn).In(time.UTC))

		merged := seqOut.SubMerge(seqIn, nil, resolutionOut, resolutionIn, eOut, eIn, submergers[0], asOf, until, 0)
		assert.Equal(t, seqIn.Until().In(time.UTC), merged.Until().In(time.UTC))
		assert.Equal(t, seqIn.AsOf(widthIn, resolutionIn).In(time.UTC), merged.AsOf(widthOut, resolutionOut).In(time.UTC))

		assert.Equal(t, 10, merged.NumPeriods(widthOut))
		for i := 0; i < 10; i++ {
			val, found := merged.ValueAt(i, eOut)
			assert.True(t, found)
			assert.EqualValues(t, 3, val)
		}

		truncated := merged.Truncate(widthOut, resolutionOut, merged.AsOf(widthOut, resolutionOut).Add(resolutionOut), merged.Until().Add(-1*resolutionOut))
		assert.Equal(t, 8, truncated.NumPeriods(widthOut))
		for i := 0; i < 8; i++ {
			val, found := truncated.ValueAt(i, eOut)
			assert.True(t, found)
			assert.EqualValues(t, 3, val)
		}

		start := merged.Until()
		end := merged.AsOf(widthOut, resolutionOut)
		assert.Nil(t, merged.Truncate(widthOut, resolutionOut, start.Add(resolutionOut), start.Add(resolutionOut*2)))
		assert.Nil(t, merged.Truncate(widthOut, resolutionOut, time.Time{}, end.Add(-1*resolutionOut)))
	}
}

func TestSequenceValue(t *testing.T) {
	e := SUM(FIELD("a"))
	v := NewFloatValue(e, epoch, 56.78)
	assert.Equal(t, epoch, v.Until().In(time.UTC))
	assert.Equal(t, 1, v.NumPeriods(e.EncodedWidth()))
	val, found := v.ValueAt(0, e)
	assert.True(t, found)
	assert.Equal(t, 56.78, val)
}

func TestSequenceConstant(t *testing.T) {
	e := CONST(5.1)
	s := Sequence(nil)
	v, _ := s.ValueAt(0, e)
	assert.EqualValues(t, 5.1, v)
	v, _ = s.ValueAtOffset(0, e)
	assert.EqualValues(t, 5.1, v)
	v, _ = s.ValueAtTime(time.Time{}, e, 0)
	assert.EqualValues(t, 5.1, v)
}

func TestSequenceSubMergePlain(t *testing.T) {
	doTestSequenceSubMerge(t, 0)
}

// func TestSequenceSubMergeStride(t *testing.T) {
// 	doTestSequenceSubMerge(t, 2)
// }

func doTestSequenceSubMerge(t *testing.T, _strideSlice int) {
	e := SUM(FIELD("a"))
	params := FloatParams(1)
	scale := 11
	inResolution := 1 * time.Minute
	strideSlice := time.Duration(_strideSlice) * inResolution
	outResolution := inResolution * time.Duration(scale)
	outPeriods := 10
	inPeriods := scale*outPeriods + 2
	asOf := time.Date(2015, 1, 1, 1, 2, 0, 0, time.UTC)
	until := asOf.Add(outResolution * time.Duration(outPeriods))

	expected := NewSequence(e.EncodedWidth(), outPeriods)
	expected.SetUntil(until)
	for i := 0; i < outPeriods; i++ {
		for j := 0; j < scale; j++ {
			if _strideSlice <= 0 || j < _strideSlice {
				expected.UpdateValueAt(i, e, params, nil)
			}
		}
	}

	var result Sequence
	submerge := e.SubMergers([]Expr{e})[0]

	// Try it with a single big in sequence
	in := NewSequence(e.EncodedWidth(), inPeriods)
	in.SetUntil(until.Add(inResolution))
	for i := 0; i < inPeriods; i++ {
		in.UpdateValueAt(i, e, params, nil)
	}
	result = result.SubMerge(in, nil, outResolution, inResolution, e, e, submerge, asOf, until, strideSlice)
	assert.Equal(t, expected.String(e, outResolution), result.String(e, outResolution))

	// Try it with a bunch of small sequences
	testSubMergeParts := func(order []int) {
		result = nil
		start := asOf.Add(-1 * inResolution)
		for _, o := range order {
			in := NewFloatValue(e, start.Add(time.Duration(o)*inResolution), 1)
			result = result.SubMerge(in, nil, outResolution, inResolution, e, e, submerge, asOf, until, strideSlice)
		}
		assert.Equal(t, expected.String(e, outResolution), result.String(e, outResolution))
	}

	order := make([]int, 0, inPeriods)
	for i := 0; i < inPeriods; i++ {
		order = append(order, i)
	}

	// try forward
	testSubMergeParts(order)

	// try reverse
	reversed := make([]int, inPeriods)
	for i, o := range order {
		reversed[inPeriods-1-i] = o
	}
	testSubMergeParts(reversed)

	// try random
	random := rand.Perm(inPeriods)
	testSubMergeParts(random)
}

func randBelow(res time.Duration) time.Duration {
	return time.Duration(-1 * rand.Intn(int(res)))
}
