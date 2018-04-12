package expr

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLN(t *testing.T) {
	doTestUnaryMath(t, "LN", math.E, 1)
}

func TestLog2(t *testing.T) {
	doTestUnaryMath(t, "LOG2", 2, 1)
}

func TestLog10(t *testing.T) {
	doTestUnaryMath(t, "LOG10", 10, 1)
}

func doTestUnaryMath(t *testing.T, name string, in float64, expected float64) {
	e, err := UnaryMath(name, CONST(in))
	if !assert.NoError(t, err) {
		return
	}
	val, _, _ := msgpacked(t, e).Get(nil)
	AssertFloatEquals(t, expected, val)
}
