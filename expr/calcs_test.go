package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestADD(t *testing.T) {
	doTestCalc(t, ADD("a", "b"), []string{"a", "b"}, 13.2)
}

func TestSUB(t *testing.T) {
	doTestCalc(t, SUB("a", "b"), []string{"a", "b"}, 4.4)
}

func TestMULT(t *testing.T) {
	doTestCalc(t, MULT("a", "b"), []string{"a", "b"}, 38.72)
}

func TestDIV(t *testing.T) {
	doTestCalc(t, DIV("a", "b"), []string{"a", "b"}, 2)
}

func TestDIVZero(t *testing.T) {
	doTestCalc(t, DIV("a", "c"), []string{"a", "c"}, 0)
}

func doTestCalc(t *testing.T, e Expr, expectedDepends []string, expected float64) {
	params := Map{
		"a": 8.8,
		"b": 4.4,
		"c": 0,
		"d": 1.1,
	}

	assert.Equal(t, expectedDepends, e.DependsOn())
	a := e.Accumulator()
	a.Update(params)
	assertFloatEquals(t, expected, a.Get())
}
