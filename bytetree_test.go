package tdb

import (
	"testing"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
	"github.com/getlantern/tdb/sql"
	"github.com/stretchr/testify/assert"
)

func TestByteTree(t *testing.T) {
	tb := &table{
		Query: sql.Query{
			Resolution: 5 * time.Second,
			Fields: []sql.Field{
				sql.Field{
					Name: "myfield",
					Expr: expr.SUM("a"),
				},
			},
		},
	}
	now := time.Now()
	truncateBefore := now.Add(-5000 * tb.Resolution)

	bt := newByteTree()
	bytesAdded := bt.update(tb, truncateBefore, []byte("test"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 1})))
	assert.Equal(t, 21, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("slow"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 2})))
	assert.Equal(t, 21, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("water"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 3})))
	assert.Equal(t, 22, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("slower"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 4})))
	assert.Equal(t, 19, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("team"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 5})))
	assert.Equal(t, 19, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("toast"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 6})))
	assert.Equal(t, 21, bytesAdded)

	bytesAdded = bt.update(tb, truncateBefore, []byte("test"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("slow"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("water"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("slower"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("team"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)
	bytesAdded = bt.update(tb, truncateBefore, []byte("toast"), newTSParams(now, bytemap.NewFloat(map[string]float64{"a": 10})))
	assert.Equal(t, 0, bytesAdded)

	val, _ := bt.root.edges[0].target.edges[0].target.edges[0].target.data[0].valueAt(0, tb.Fields[0])
	assert.EqualValues(t, 11, val, "Wrong value for 'test'")
	val, _ = bt.root.edges[0].target.edges[0].target.edges[1].target.data[0].valueAt(0, tb.Fields[0])
	assert.EqualValues(t, 15, val, "Wrong value for 'team'")
	val, _ = bt.root.edges[0].target.edges[1].target.data[0].valueAt(0, tb.Fields[0])
	assert.EqualValues(t, 16, val, "Wrong value for 'toast'")
	val, _ = bt.root.edges[1].target.data[0].valueAt(0, tb.Fields[0])
	assert.EqualValues(t, 12, val, "Wrong value for 'slow'")
	val, _ = bt.root.edges[1].target.edges[0].target.data[0].valueAt(0, tb.Fields[0])
	assert.EqualValues(t, 14, val, "Wrong value for 'slower'")
	val, _ = bt.root.edges[2].target.data[0].valueAt(0, tb.Fields[0])
	assert.EqualValues(t, 13, val, "Wrong value for 'water'")

}
