package bytetree

import (
	"testing"
	"time"

	"github.com/getlantern/zenodb/encoding"
	. "github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
)

const ctx = 56

var (
	epoch = time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
)

func TestByteTree(t *testing.T) {
	resolutionOut := 10 * time.Second
	resolutionIn := 1 * time.Second

	asOf := epoch.Add(-1 * resolutionOut)
	until := epoch

	eOut := ADD(SUM(FIELD("a")), SUM(FIELD("b")))
	eA := SUM(FIELD("a"))
	eB := SUM(FIELD("b"))

	bt := New([]Expr{eOut}, []Expr{eA, eB}, resolutionOut, resolutionIn, asOf, until)
	bt.Update([]byte("test"), []encoding.Sequence{encoding.NewValue(eA, epoch, 1), encoding.NewValue(eB, epoch, 1)}, nil)
	assert.Equal(t, 1, bt.Length())
	bt.Update([]byte("slow"), []encoding.Sequence{encoding.NewValue(eA, epoch, 2), encoding.NewValue(eB, epoch, 2)}, nil)
	assert.Equal(t, 2, bt.Length())
	bt.Update([]byte("water"), []encoding.Sequence{encoding.NewValue(eA, epoch, 3), encoding.NewValue(eB, epoch, 3)}, nil)
	assert.Equal(t, 3, bt.Length())
	bt.Update([]byte("slower"), []encoding.Sequence{encoding.NewValue(eA, epoch, 4), encoding.NewValue(eB, epoch, 4)}, nil)
	assert.Equal(t, 4, bt.Length())
	bt.Update([]byte("team"), []encoding.Sequence{encoding.NewValue(eA, epoch, 5), encoding.NewValue(eB, epoch, 5)}, nil)
	assert.Equal(t, 5, bt.Length())
	bt.Update([]byte("toast"), []encoding.Sequence{encoding.NewValue(eA, epoch, 6), encoding.NewValue(eB, epoch, 6)}, nil)
	assert.Equal(t, 6, bt.Length())

	bt.Update([]byte("test"), []encoding.Sequence{encoding.NewValue(eA, epoch, 10), encoding.NewValue(eB, epoch, 10)}, nil)
	assert.Equal(t, 6, bt.Length())
	bt.Update([]byte("slow"), []encoding.Sequence{encoding.NewValue(eA, epoch, 10), encoding.NewValue(eB, epoch, 10)}, nil)
	assert.Equal(t, 6, bt.Length())
	bt.Update([]byte("water"), []encoding.Sequence{encoding.NewValue(eA, epoch, 10), encoding.NewValue(eB, epoch, 10)}, nil)
	assert.Equal(t, 6, bt.Length())
	bt.Update([]byte("slower"), []encoding.Sequence{encoding.NewValue(eA, epoch, 10), encoding.NewValue(eB, epoch, 10)}, nil)
	assert.Equal(t, 6, bt.Length())
	bt.Update([]byte("team"), []encoding.Sequence{encoding.NewValue(eA, epoch, 10), encoding.NewValue(eB, epoch, 10)}, nil)
	assert.Equal(t, 6, bt.Length())
	bt.Update([]byte("toast"), []encoding.Sequence{encoding.NewValue(eA, epoch, 10), encoding.NewValue(eB, epoch, 10)}, nil)
	assert.Equal(t, 6, bt.Length())

	// This should be ignored because it's outside of the time range
	bt.Update([]byte("test"), []encoding.Sequence{encoding.NewValue(eA, epoch.Add(-1*resolutionOut), 50), encoding.NewValue(eB, epoch.Add(1*resolutionOut), 10)}, nil)

	// Check tree twice with different contexts to make sure removals don't affect
	// other contexts.
	checkTree(ctx, t, bt, eOut)
	checkTree(98, t, bt, eOut)

	// Copy tree and check again
	checkTree(99, t, bt.Copy(), eOut)
}

func checkTree(ctx int64, t *testing.T, bt *Tree, e Expr) {
	walkedValues := 0
	bt.Walk(ctx, func(key []byte, data []encoding.Sequence) bool {
		if assert.Len(t, data, 1) {
			walkedValues++
			val, _ := data[0].ValueAt(0, e)
			switch string(key) {
			case "test":
				assert.EqualValues(t, 22, val, "test")
			case "slow":
				assert.EqualValues(t, 24, val, "slow")
			case "water":
				assert.EqualValues(t, 26, val, "water")
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
		return true
	})
	assert.Equal(t, 6, walkedValues)

	val, _ := bt.Remove(ctx, []byte("test"))[0].ValueAt(0, e)
	assert.EqualValues(t, 22, val)
	val, _ = bt.Remove(ctx, []byte("slow"))[0].ValueAt(0, e)
	assert.EqualValues(t, 24, val)
	val, _ = bt.Remove(ctx, []byte("water"))[0].ValueAt(0, e)
	assert.EqualValues(t, 26, val)
	val, _ = bt.Remove(ctx, []byte("slower"))[0].ValueAt(0, e)
	assert.EqualValues(t, 28, val)
	val, _ = bt.Remove(ctx, []byte("team"))[0].ValueAt(0, e)
	assert.EqualValues(t, 30, val)
	val, _ = bt.Remove(ctx, []byte("toast"))[0].ValueAt(0, e)
	assert.EqualValues(t, 32, val)
	assert.Nil(t, bt.Remove(ctx, []byte("unknown")))
}
