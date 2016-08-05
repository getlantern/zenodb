package expr

import (
	"testing"
)

func TestCombined(t *testing.T) {
	avgA := AVG("a")
	avgB := AVG("b")
	mult := MULT(avgA, avgB)
	count := COUNT("b")
	e := DIV(mult, count)
	params1 := Map{
		"a": 2,
		"b": 10,
	}
	params2 := Map{
		"a": 4,
		"b": 20,
	}
	params3 := Map{
		"a": 0,
		"b": 3,
	}

	b := make([]byte, e.EncodedWidth())
	e.Update(b, params1)
	e.Update(b, params2)
	val, _, _ := e.Get(b)
	assertFloatEquals(t, 22.5, val)

	b2 := make([]byte, e.EncodedWidth())
	e.Update(b2, params3)
	b3 := make([]byte, e.EncodedWidth())
	e.Merge(b3, b, b2)
	val, _, _ = e.Get(b3)
	assertFloatEquals(t, 7.33333333, val)

	// Test SubMerge
	bavgA := make([]byte, avgA.EncodedWidth())
	bavgB := make([]byte, avgB.EncodedWidth())
	bmult := make([]byte, mult.EncodedWidth())
	bcount := make([]byte, count.EncodedWidth())
	avgA.Update(bavgA, params1)
	avgB.Update(bavgB, params1)
	mult.Update(bmult, params1)
	count.Update(bcount, params1)
	avgA.Update(bavgA, params2)
	avgB.Update(bavgB, params2)
	mult.Update(bmult, params2)
	count.Update(bcount, params2)
	avgA.Update(bavgA, params3)
	avgB.Update(bavgB, params3)
	mult.Update(bmult, params3)
	count.Update(bcount, params3)
	be := make([]byte, e.EncodedWidth())
	fields := []Expr{avgA, avgB, mult, count}
	data := [][]byte{bavgA, bavgB, bmult, bcount}
	sms := e.SubMergers(fields)
	for i, sm := range sms {
		if sm != nil {
			sm(be, data[i])
		}
	}
	val, _, _ = e.Get(be)
	assertFloatEquals(t, 7.33333333, val)
}
