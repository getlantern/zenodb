package bytetree

import (
	"testing"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/encoding"
	. "github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
)

const ctx = 56

var (
	epoch = time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
)

func TestByteTreeSubMerge(t *testing.T) {
	doTest(t, func(bt *Tree, resolutionOut time.Duration, eA Expr, eB Expr) {
		// Updates that create new keys
		bt.Update([]byte("test"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 1), encoding.NewFloatValue(eB, epoch, 1)}, nil, nil)
		assert.Equal(t, 1, bt.Length())
		bt.Update([]byte("slow"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 2), encoding.NewFloatValue(eB, epoch, 2)}, nil, nil)
		assert.Equal(t, 2, bt.Length())
		bt.Update(nil, []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 3), encoding.NewFloatValue(eB, epoch, 3)}, nil, nil)
		assert.Equal(t, 3, bt.Length())
		bt.Update([]byte("slower"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 4), encoding.NewFloatValue(eB, epoch, 4)}, nil, nil)
		assert.Equal(t, 4, bt.Length())
		bt.Update([]byte("team"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 5), encoding.NewFloatValue(eB, epoch, 5)}, nil, nil)
		assert.Equal(t, 5, bt.Length())
		bt.Update([]byte("toast"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 6), encoding.NewFloatValue(eB, epoch, 6)}, nil, nil)
		assert.Equal(t, 6, bt.Length())

		// Updates to existing keys
		bt.Update([]byte("test"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 10), encoding.NewFloatValue(eB, epoch, 10)}, nil, nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update([]byte("slow"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 10), encoding.NewFloatValue(eB, epoch, 10)}, nil, nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update(nil, []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 10), encoding.NewFloatValue(eB, epoch, 10)}, nil, nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update([]byte("slower"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 10), encoding.NewFloatValue(eB, epoch, 10)}, nil, nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update([]byte("team"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 10), encoding.NewFloatValue(eB, epoch, 10)}, nil, nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update([]byte("toast"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch, 10), encoding.NewFloatValue(eB, epoch, 10)}, nil, nil)
		assert.Equal(t, 6, bt.Length())

		// This should be ignored because it's outside of the time range
		bt.Update([]byte("test"), []encoding.Sequence{encoding.NewFloatValue(eA, epoch.Add(-1*resolutionOut), 50), encoding.NewFloatValue(eB, epoch.Add(1*resolutionOut), 10)}, nil, nil)
	})
}

func TestByteTreeUpdate(t *testing.T) {
	doTest(t, func(bt *Tree, resolutionOut time.Duration, eA Expr, eB Expr) {
		// Updates that create new keys
		bt.Update([]byte("test"), nil, params(1, 1), nil)
		assert.Equal(t, 1, bt.Length())
		bt.Update([]byte("slow"), nil, params(2, 2), nil)
		assert.Equal(t, 2, bt.Length())
		bt.Update(nil, nil, params(3, 3), nil)
		assert.Equal(t, 3, bt.Length())
		bt.Update([]byte("slower"), nil, params(4, 4), nil)
		assert.Equal(t, 4, bt.Length())
		bt.Update([]byte("team"), nil, params(5, 5), nil)
		assert.Equal(t, 5, bt.Length())
		bt.Update([]byte("toast"), nil, params(6, 6), nil)
		assert.Equal(t, 6, bt.Length())

		// Updates to existing keys
		bt.Update([]byte("test"), nil, params(10, 10), nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update([]byte("slow"), nil, params(10, 10), nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update(nil, nil, params(10, 10), nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update([]byte("slower"), nil, params(10, 10), nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update([]byte("team"), nil, params(10, 10), nil)
		assert.Equal(t, 6, bt.Length())
		bt.Update([]byte("toast"), nil, params(10, 10), nil)
		assert.Equal(t, 6, bt.Length())

		bt.Update([]byte("test"), nil, tsParams(epoch.Add(-1*resolutionOut), 50, 10), nil)
	})
}

func doTest(t *testing.T, populate func(bt *Tree, resolutionOut time.Duration, eA Expr, eB Expr)) {
	resolutionOut := 10 * time.Second
	resolutionIn := 1 * time.Second

	asOf := epoch.Add(-1 * resolutionOut)
	until := epoch

	eOut := ADD(SUM(FIELD("a")), SUM(FIELD("b")))
	eA := SUM(FIELD("a"))
	eB := SUM(FIELD("b"))

	// First test submerging

	bt := New([]Expr{eOut}, []Expr{eA, eB}, resolutionOut, resolutionIn, asOf, until)
	populate(bt, resolutionOut, eA, eB)

	// Check tree twice with different contexts to make sure removals don't affect
	// other contexts.
	checkTree(ctx, t, bt, eOut)
	checkTree(98, t, bt, eOut)

	// Copy tree and check again
	checkTree(99, t, bt.Copy(), eOut)
}

func tsParams(ts time.Time, a float64, b float64) encoding.TSParams {
	return encoding.NewTSParams(ts, bytemap.NewFloat(map[string]float64{"a": a, "b": b}))
}

func params(a float64, b float64) encoding.TSParams {
	return tsParams(epoch, a, b)
}

func checkTree(ctx int64, t *testing.T, bt *Tree, e Expr) {
	walkedValues := 0
	bt.Walk(ctx, func(key []byte, data []encoding.Sequence) (bool, bool, error) {
		if assert.Len(t, data, 1) {
			walkedValues++
			val, _ := data[0].ValueAt(0, e)
			switch string(key) {
			case "test":
				assert.EqualValues(t, 22, val, "test")
			case "slow":
				assert.EqualValues(t, 24, val, "slow")
			case "":
				assert.EqualValues(t, 26, val, "")
			case "slower":
				assert.EqualValues(t, 28, val, "slower")
			case "team":
				assert.EqualValues(t, 30, val, "team")
			case "toast":
				assert.EqualValues(t, 32, val, "toast")
			default:
				assert.Fail(t, "Unknown key", string(key))
			}
		}
		return true, true, nil
	})
	assert.Equal(t, 6, walkedValues)

	val, _ := bt.Remove(ctx, []byte("test"))[0].ValueAt(0, e)
	assert.EqualValues(t, 22, val)
	val, _ = bt.Remove(ctx, []byte("slow"))[0].ValueAt(0, e)
	assert.EqualValues(t, 24, val)
	val, _ = bt.Remove(ctx, nil)[0].ValueAt(0, e)
	assert.EqualValues(t, 26, val)
	val, _ = bt.Remove(ctx, []byte("slower"))[0].ValueAt(0, e)
	assert.EqualValues(t, 28, val)
	val, _ = bt.Remove(ctx, []byte("team"))[0].ValueAt(0, e)
	assert.EqualValues(t, 30, val)
	val, _ = bt.Remove(ctx, []byte("toast"))[0].ValueAt(0, e)
	assert.EqualValues(t, 32, val)
	assert.Nil(t, bt.Remove(ctx, []byte("unknown")))
}
