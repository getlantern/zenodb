package expr

import (
	"testing"

	"github.com/Knetic/govaluate"
	"github.com/stretchr/testify/assert"
)

func TestSUM(t *testing.T) {
	doTestAggregate(t, SUM("a", nil), 13.2)
}

func TestCOUNT(t *testing.T) {
	doTestAggregate(t, COUNT("b", nil), 3)
}

func TestAVG(t *testing.T) {
	doTestAggregate(t, AVG("a", nil), 6.6)
}

func TestSUMConditional(t *testing.T) {
	cond, err := govaluate.NewEvaluableExpression("i")
	if !assert.NoError(t, err) {
		return
	}
	doTestAggregate(t, SUM("b", cond), 1)
}

func TestValidateAggregate(t *testing.T) {
	sum := SUM(MULT(CONST(1), CONST(2)), nil)
	assert.Error(t, sum.Validate())
	avg := AVG(MULT(CONST(1), CONST(2)), nil)
	assert.Error(t, avg.Validate())
	ok := SUM(CONST(1), nil)
	assert.NoError(t, ok.Validate())
	ok2 := AVG(FIELD("b"), nil)
	assert.NoError(t, ok2.Validate())
}

func doTestAggregate(t *testing.T, e Expr, expected float64) {
	params1 := Map{
		"a": 4.4,
	}
	md1 := govaluate.MapParameters{}
	params2 := Map{
		"a": 8.8,
		"b": 0.8,
	}
	md2 := govaluate.MapParameters{
		"i": true,
	}
	params3 := Map{
		"b": 0.1,
	}
	md3 := govaluate.MapParameters{}
	params4 := Map{
		"b": 0.2,
		"i": 1,
	}
	md4 := govaluate.MapParameters{
		"i": true,
	}

	b := make([]byte, e.EncodedWidth())
	e.Update(b, params1, md1)
	e.Update(b, params2, md2)
	e.Update(b, params3, md3)
	e.Update(b, params4, md4)
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
	e.Merge(b3, b1, b2, nil)
	val, wasSet, _ = e.Get(b3)
	if assert.True(t, wasSet) {
		assertFloatEquals(t, expected, val)
	}
}
