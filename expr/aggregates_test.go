package expr

import (
	"testing"

	"github.com/getlantern/goexpr"
	"github.com/stretchr/testify/assert"
)

func TestSUM(t *testing.T) {
	doTestAggregate(t, SUM(boundedA()), 13.2)
}

func TestMIN(t *testing.T) {
	doTestAggregate(t, MIN(boundedA()), 4.4)
}

func TestMAX(t *testing.T) {
	doTestAggregate(t, MAX(boundedA()), 8.8)
}

func TestCOUNT(t *testing.T) {
	doTestAggregate(t, COUNT("b"), 3)
}

func TestAVG(t *testing.T) {
	doTestAggregate(t, AVG(boundedA()), 6.6)
}

func TestSUMConditional(t *testing.T) {
	ex, err := IF(goexpr.Param("i"), SUM("b"))
	if !assert.NoError(t, err) {
		return
	}
	doTestAggregate(t, ex, 1)
}

func TestValidateAggregate(t *testing.T) {
	sum := SUM(MULT(CONST(1), CONST(2)))
	assert.Error(t, sum.Validate())
	avg := AVG(MULT(CONST(1), CONST(2)))
	assert.Error(t, avg.Validate())
	ok := SUM(CONST(1))
	assert.NoError(t, ok.Validate())
	ok2 := AVG(FIELD("b"))
	assert.NoError(t, ok2.Validate())
}

func boundedA() Expr {
	return BOUNDED("a", 0.1, 8.8)
}

func doTestAggregate(t *testing.T, e Expr, expected float64) {
	params1 := Map{
		"a": 4.4,
	}
	md1 := goexpr.MapParams{}
	params2 := Map{
		"a": 8.8,
		"b": 0.8,
	}
	md2 := goexpr.MapParams{
		"i": true,
	}
	params3 := Map{
		"b": 0.1,
	}
	md3 := goexpr.MapParams{}
	params4 := Map{
		"b": 0.2,
		"i": 1,
	}
	md4 := goexpr.MapParams{
		"i": true,
	}
	// params5 and params6 will be ignored because they fall outside of the bounds
	params5 := Map{
		"a": 0.01,
	}
	params6 := Map{
		"a": 8.9,
	}
	b := make([]byte, e.EncodedWidth())
	e.Update(b, params1, md1)
	e.Update(b, params2, md2)
	e.Update(b, params3, md3)
	e.Update(b, params4, md4)
	e.Update(b, params5, md1)
	e.Update(b, params6, md1)
	val, wasSet, _ := e.Get(b)
	if assert.True(t, wasSet) {
		assertFloatEquals(t, expected, val)
	}

	// Test Merging
	b1 := make([]byte, e.EncodedWidth())
	e.Update(b1, params1, md1)
	e.Update(b1, params2, md2)
	b2 := make([]byte, e.EncodedWidth())
	e.Update(b2, params3, md3)
	e.Update(b2, params4, md4)
	b3 := make([]byte, e.EncodedWidth())
	e.Merge(b3, b1, b2)
	val, wasSet, _ = e.Get(b3)
	if assert.True(t, wasSet) {
		assertFloatEquals(t, expected, val)
	}
}
