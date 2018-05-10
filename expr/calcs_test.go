package expr

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestADD(t *testing.T) {
	doTestCalc(t, ADD("a", "b"), 13.2)
}

func TestSUB(t *testing.T) {
	doTestCalc(t, SUB("a", "b"), 4.4)
}

func TestMULT(t *testing.T) {
	doTestCalc(t, MULT("a", "b"), 38.72)
}

func TestDIV(t *testing.T) {
	doTestCalc(t, DIV("a", "b"), 2)
}

func TestDIVZero(t *testing.T) {
	doTestCalc(t, DIV("a", "c"), math.MaxFloat64)
}

func TestDIVZeroZero(t *testing.T) {
	doTestCalc(t, DIV("c", "c"), 0)
}

func TestValidateBinary(t *testing.T) {
	bad := MULT(FIELD("a"), FIELD("b"))
	assert.Error(t, bad.Validate())
	dok := bad.DeAggregate()
	assert.NoError(t, dok.Validate())
	ok := MULT(CONST(1), SUM(FIELD("b")))
	assert.NoError(t, ok.Validate())
	ok2 := MULT(CONST(1), AVG(FIELD("b")))
	assert.NoError(t, ok2.Validate())
	ok3 := MULT(CONST(1), ADD(AVG(FIELD("b")), CONST(3)))
	assert.NoError(t, ok3.Validate())
	ok4 := MULT(CONST(1), ADD(AVG(FIELD("b")), GT(CONST(3), SUM("c"))))
	assert.NoError(t, ok4.Validate())
}

func doTestCalc(t *testing.T, e Expr, expected float64) {
	e = msgpacked(t, e)
	params := Map{
		"a": 8.8,
		"b": 4.4,
		"c": 0,
		"d": 1.1,
	}

	b := make([]byte, e.EncodedWidth())
	_, val, _ := e.Update(b, params, nil)
	AssertFloatEquals(t, expected, val)
}
