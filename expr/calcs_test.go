package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdd(t *testing.T) {
	doTestCalc(t, Add("a", "b"), []string{"a", "b"}, 13.2)
}

func TestSub(t *testing.T) {
	doTestCalc(t, Sub("a", "b"), []string{"a", "b"}, 4.4)
}

func TestMult(t *testing.T) {
	doTestCalc(t, Mult("a", "b"), []string{"a", "b"}, 38.72)
}

func TestDiv(t *testing.T) {
	doTestCalc(t, Div("a", "b"), []string{"a", "b"}, 2)
}

func TestDivZero(t *testing.T) {
	doTestCalc(t, Div("a", "c"), []string{"a", "c"}, 0)
}

func doTestCalc(t *testing.T, e Expr, expectedDepends []string, expected float64) {
	params := Map{
		"a": Float(8.8),
		"b": Float(4.4),
		"c": Float(0),
	}

	assert.Equal(t, expectedDepends, e.DependsOn())
	a := e.Accumulator()
	a.Update(params)
	assertFloatEquals(t, expected, a.Get())
}
