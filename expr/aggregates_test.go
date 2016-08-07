package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSUM(t *testing.T) {
	doTestAggregate(t, SUM("a"), 13.2)
}

func TestCOUNT(t *testing.T) {
	doTestAggregate(t, COUNT("b"), 3)
}

func TestCOUNTConstant(t *testing.T) {
	doTestAggregate(t, COUNT(CONST(1)), 4)
}

func TestAVG(t *testing.T) {
	doTestAggregate(t, AVG("a"), 6.6)
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

func doTestAggregate(t *testing.T, e Expr, expected float64) {
	params1 := Map{
		"a": 4.4,
	}
	params2 := Map{
		"a": 8.8,
		"b": 0.8,
	}
	params3 := Map{
		"b": 0.1,
	}
	params4 := Map{
		"b": 0.2,
	}

	b := make([]byte, e.EncodedWidth())
	e.Update(b, params1)
	e.Update(b, params2)
	e.Update(b, params3)
	e.Update(b, params4)
	val, wasSet, _ := e.Get(b)
	if assert.True(t, wasSet) {
		assertFloatEquals(t, expected, val)
	}

	// Test Merging
	b1 := make([]byte, e.EncodedWidth())
	e.Update(b1, params1)
	e.Update(b1, params2)
	b2 := make([]byte, e.EncodedWidth())
	e.Update(b2, params3)
	e.Update(b2, params4)
	b3 := make([]byte, e.EncodedWidth())
	e.Merge(b3, b1, b2)
	val, wasSet, _ = e.Get(b3)
	if assert.True(t, wasSet) {
		assertFloatEquals(t, expected, val)
	}
}
