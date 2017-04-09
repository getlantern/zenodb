package expr

import (
	"testing"

	"github.com/getlantern/goexpr"
	"github.com/stretchr/testify/assert"
	"gopkg.in/vmihailenco/msgpack.v2"
)

func TestCombined(t *testing.T) {
	avgA := AVG("a")
	avgB := AVG("b")
	mult := MULT(avgA, avgB)
	count := COUNT("b")
	ge, err := goexpr.Binary("=", goexpr.Constant(0), goexpr.Constant(0))
	if !assert.NoError(t, err) {
		return
	}
	ie, err := IF(ge, DIV(mult, count))
	if !assert.NoError(t, err) {
		return
	}
	e := msgpacked(t, ie)
	params1 := Map{
		"a": 2,
		"b": 10,
	}
	md1 := goexpr.MapParams{}
	params2 := Map{
		"a": 4,
		"b": 20,
	}
	md2 := goexpr.MapParams{}
	params3 := Map{
		"a": 0,
		"b": 3,
	}
	md3 := goexpr.MapParams{}

	b := make([]byte, e.EncodedWidth())
	e.Update(b, params1, md1)
	e.Update(b, params2, md2)
	val, _, _ := e.Get(b)
	assertFloatEquals(t, 22.5, val)

	b2 := make([]byte, e.EncodedWidth())
	e.Update(b2, params3, md3)
	b3 := make([]byte, e.EncodedWidth())
	e.Merge(b3, b, b2)
	val, _, _ = e.Get(b3)
	assertFloatEquals(t, 7.33333333, val)

	// Test SubMerge
	bavgA := make([]byte, avgA.EncodedWidth())
	bavgB := make([]byte, avgB.EncodedWidth())
	bmult := make([]byte, mult.EncodedWidth())
	bcount := make([]byte, count.EncodedWidth())
	avgA.Update(bavgA, params1, md1)
	avgB.Update(bavgB, params1, md1)
	mult.Update(bmult, params1, md1)
	count.Update(bcount, params1, md1)
	avgA.Update(bavgA, params2, md2)
	avgB.Update(bavgB, params2, md2)
	mult.Update(bmult, params2, md2)
	count.Update(bcount, params2, md2)
	avgA.Update(bavgA, params3, md3)
	avgB.Update(bavgB, params3, md3)
	mult.Update(bmult, params3, md3)
	count.Update(bcount, params3, md3)
	be := make([]byte, e.EncodedWidth())
	fields := []Expr{avgA, avgB, mult, count}
	data := [][]byte{bavgA, bavgB, bmult, bcount}
	sms := e.SubMergers(fields)
	for i, sm := range sms {
		if sm != nil {
			sm(be, data[i], nil)
		}
	}
	val, _, _ = e.Get(be)
	assertFloatEquals(t, 7.33333333, val)
}

func msgpacked(t *testing.T, e Expr) Expr {
	b, err := msgpack.Marshal(e)
	if !assert.NoError(t, err) {
		return e
	}
	var e2 interface{}
	err = msgpack.Unmarshal(b, &e2)
	if !assert.NoError(t, err) {
		return e
	}
	return e2.(Expr)
}
