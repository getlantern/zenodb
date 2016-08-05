package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSUM(t *testing.T) {
	doTestAggregate(t, SUM("a"), 13.2)
}

func TestCOUNT(t *testing.T) {
	doTestAggregate(t, COUNT("b"), 1)
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
		"b": 1.1,
	}

	b := make([]byte, e.EncodedWidth())
	e.Update(b, params1)
	e.Update(b, params2)
	val, wasSet, _ := e.Get(b)
	if assert.True(t, wasSet) {
		assertFloatEquals(t, expected, val)
	}
}
