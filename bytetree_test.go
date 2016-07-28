package tdb

import (
	"testing"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
	"github.com/getlantern/tdb/sql"
	"github.com/stretchr/testify/assert"
)

const ctx = 56

func TestByteTree(t *testing.T) {
	e := expr.SUM("a")

	tb := &table{
		Query: sql.Query{
			Resolution: 5 * time.Second,
			Fields: []sql.Field{
				sql.Field{
					Name: "myfield",
					Expr: e,
				},
			},
		},
	}
	now := time.Now()
	truncateBefore := now.Add(-5000 * tb.Resolution)

	bt := newByteTree()
	bytesAdded := bt.update(tb, truncateBefore, []byte("test"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 1})))
	assert.Equal(t, 21, bytesAdded)
	assert.Equal(t, 1, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("slow"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 2})))
	assert.Equal(t, 21, bytesAdded)
	assert.Equal(t, 2, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("water"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 3})))
	assert.Equal(t, 22, bytesAdded)
	assert.Equal(t, 3, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("slower"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 4})))
	assert.Equal(t, 19, bytesAdded)
	assert.Equal(t, 4, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("team"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 5})))
	assert.Equal(t, 19, bytesAdded)
	assert.Equal(t, 5, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("toast"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 6})))
	assert.Equal(t, 21, bytesAdded)
	assert.Equal(t, 6, bt.length())

	bytesAdded = bt.update(tb, truncateBefore, []byte("test"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("slow"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("water"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("slower"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("team"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.length())
	bytesAdded = bt.update(tb, truncateBefore, []byte("toast"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	assert.Equal(t, 6, bt.length())

	// Check tree twice with different contexts to make sure removals don't affect
	// other contexts.
	checkTree(ctx, t, bt, e)
	checkTree(98, t, bt, e)
}

func checkTree(ctx int64, t *testing.T, bt *tree, e expr.Expr) {
	walkedValues := 0
	bt.walk(ctx, func(key []byte, data []sequence) bool {
		if assert.Len(t, data, 1) {
			walkedValues++
			val, _ := data[0].valueAt(0, e)
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

	val, _ := bt.remove(ctx, []byte("test"))[0].valueAt(0, e)
	assert.EqualValues(t, 11, val)
	val, _ = bt.remove(ctx, []byte("slow"))[0].valueAt(0, e)
	assert.EqualValues(t, 12, val)
	val, _ = bt.remove(ctx, []byte("water"))[0].valueAt(0, e)
	assert.EqualValues(t, 13, val)
	val, _ = bt.remove(ctx, []byte("slower"))[0].valueAt(0, e)
	assert.EqualValues(t, 14, val)
	val, _ = bt.remove(ctx, []byte("team"))[0].valueAt(0, e)
	assert.EqualValues(t, 15, val)
	val, _ = bt.remove(ctx, []byte("toast"))[0].valueAt(0, e)
	assert.EqualValues(t, 16, val)
	assert.Nil(t, bt.remove(ctx, []byte("unknown")))
}
