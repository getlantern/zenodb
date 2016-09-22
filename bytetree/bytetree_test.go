package bytetree

import (
	"testing"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/sql"
	"github.com/stretchr/testify/assert"
)

const ctx = 56

func TestByteTree(t *testing.T) {
	e := expr.SUM("a")

	fields := []sql.Field{sql.NewField("myfield", e)}
	resolution := 5 * time.Second
	now := time.Now()
	truncateBefore := now.Add(-5000 * resolution)

	bt := New()
	bytesAdded := bt.Update(fields, resolution, truncateBefore, []byte("test"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 1})), nil)
	assert.Equal(t, 21, bytesAdded)
	assert.Equal(t, 1, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("slow"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 2})), nil)
	assert.Equal(t, 21, bytesAdded)
	assert.Equal(t, 2, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("water"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 3})), nil)
	assert.Equal(t, 22, bytesAdded)
	assert.Equal(t, 3, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("slower"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 4})), nil)
	assert.Equal(t, 19, bytesAdded)
	assert.Equal(t, 4, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("team"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 5})), nil)
	assert.Equal(t, 19, bytesAdded)
	assert.Equal(t, 5, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("toast"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 6})), nil)
	assert.Equal(t, 21, bytesAdded)
	assert.Equal(t, 6, bt.Length())

	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("test"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})), nil)
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("slow"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})), nil)
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("water"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})), nil)
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("slower"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})), nil)
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("team"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})), nil)
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.Length())
	bytesAdded = bt.Update(fields, resolution, truncateBefore, []byte("toast"), encoding.NewTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})), nil)
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.Length())

	// Check tree twice with different contexts to make sure removals don't affect
	// other contexts.
	checkTree(ctx, t, bt, e)
	checkTree(98, t, bt, e)

	// Copy tree and check again
	checkTree(99, t, bt.Copy(), e)
}

func checkTree(ctx int64, t *testing.T, bt *Tree, e expr.Expr) {
	walkedValues := 0
	bt.Walk(ctx, func(key []byte, data []encoding.Sequence) bool {
		if assert.Len(t, data, 1) {
			walkedValues++
			val, _ := data[0].ValueAt(0, e)
			switch string(key) {
			case "test":
				assert.EqualValues(t, 11, val)
			case "slow":
				assert.EqualValues(t, 12, val)
			case "water":
				assert.EqualValues(t, 13, val)
			case "slower":
				assert.EqualValues(t, 14, val)
			case "team":
				assert.EqualValues(t, 15, val)
			case "toast":
				assert.EqualValues(t, 16, val)
			default:
				assert.Fail(t, "Unknown key", string(key))
			}
		}
		return true
	})
	assert.Equal(t, 6, walkedValues)

	val, _ := bt.Remove(ctx, []byte("test"))[0].ValueAt(0, e)
	assert.EqualValues(t, 11, val)
	val, _ = bt.Remove(ctx, []byte("slow"))[0].ValueAt(0, e)
	assert.EqualValues(t, 12, val)
	val, _ = bt.Remove(ctx, []byte("water"))[0].ValueAt(0, e)
	assert.EqualValues(t, 13, val)
	val, _ = bt.Remove(ctx, []byte("slower"))[0].ValueAt(0, e)
	assert.EqualValues(t, 14, val)
	val, _ = bt.Remove(ctx, []byte("team"))[0].ValueAt(0, e)
	assert.EqualValues(t, 15, val)
	val, _ = bt.Remove(ctx, []byte("toast"))[0].ValueAt(0, e)
	assert.EqualValues(t, 16, val)
	assert.Nil(t, bt.Remove(ctx, []byte("unknown")))
}
