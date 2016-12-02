package encoding

import (
	"time"

	. "github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	epoch = time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
)

func TestSequenceFull(t *testing.T) {
	resolutionOut := 10 * time.Second
	resolutionIn := 1 * time.Second

	eOut := ADD(SUM(FIELD("a")), SUM(FIELD("b")))
	eIn := SUM(FIELD("a"))
	eB := SUM(FIELD("b"))
	submergers := eOut.SubMergers([]Expr{eIn, eB})

	widthOut := eOut.EncodedWidth()
	widthIn := eIn.EncodedWidth()
	seqIn := NewSequence(widthIn, 100)

	params := FloatParams(1)
	for i := 0; i < 100; i++ {
		seqIn.UpdateValueAt(i, eIn, params, nil)
	}

	for _, seqOut := range []Sequence{NewSequence(widthOut, 5), nil} {
		if seqOut != nil {
			seqOut.SetUntil(epoch.Add(-2 * resolutionOut))
			assert.Equal(t, epoch.Add(-2*resolutionOut).In(time.UTC), seqOut.Until().In(time.UTC))
			assert.Equal(t, epoch.Add(-7*resolutionOut).In(time.UTC), seqOut.AsOf(widthOut, resolutionOut).In(time.UTC))
		}

		seqIn.SetUntil(epoch)
		assert.Equal(t, epoch, seqIn.Until().In(time.UTC))
		assert.Equal(t, epoch.Add(-100*resolutionIn).In(time.UTC), seqIn.AsOf(widthIn, resolutionIn).In(time.UTC))

		merged := seqOut.SubMerge(seqIn, nil, resolutionOut, resolutionIn, eOut, eIn, submergers[0], time.Time{}, time.Time{})
		assert.Equal(t, RoundTime(seqIn.Until().In(time.UTC), resolutionOut), merged.Until().In(time.UTC))
		assert.Equal(t, RoundTime(seqIn.AsOf(widthIn, resolutionIn).In(time.UTC), resolutionOut), merged.AsOf(widthOut, resolutionOut).In(time.UTC))

		assert.Equal(t, 10, merged.NumPeriods(widthOut))
		for i := 0; i < 10; i++ {
			val, found := merged.ValueAt(i, eOut)
			assert.True(t, found)
			assert.EqualValues(t, 10, val)
		}

		start := merged.Until()
		end := merged.AsOf(widthOut, resolutionOut)
		assert.Nil(t, merged.Truncate(widthOut, resolutionOut, start.Add(resolutionOut), start.Add(resolutionOut*2)))
		assert.Nil(t, merged.Truncate(widthOut, resolutionOut, time.Time{}, end.Add(-1*resolutionOut)))
	}
}

func TestSequenceValue(t *testing.T) {
	e := SUM(FIELD("a"))
	v := NewValue(e, epoch, 56.78)
	assert.Equal(t, epoch, v.Until().In(time.UTC))
	assert.Equal(t, 1, v.NumPeriods(e.EncodedWidth()))
	val, found := v.ValueAt(0, e)
	assert.True(t, found)
	assert.Equal(t, 56.78, val)
}
