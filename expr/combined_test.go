package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCombined(t *testing.T) {
	e, err := JS(`MULT(AVG(SUB(ADD(DIV("a", "b"), 1), 0.5)), 2)`)
	if !assert.NoError(t, err, "Unable to parse JS expression") {
		return
	}
	params1 := Map{
		"a": 8.8,
		"b": 4.4,
	}
	params2 := Map{
		"a": 20,
		"b": 5,
	}
	params3 := Map{
		"a": 0,
		"b": 1,
	}

	assert.Equal(t, []string{"a", "b"}, e.DependsOn())
	a := e.Accumulator()
	a.Update(params1)
	a.Update(params2)
	assertFloatEquals(t, 7, a.Get())

	assert.Equal(t, a.EncodedWidth(), len(Encoded(a)))
	b := append(Encoded(a), Encoded(a)...)
	rta := e.Accumulator()
	rtb := e.Accumulator()
	b = rta.InitFrom(b)
	rtb.InitFrom(b)
	assertFloatEquals(t, 7, rta.Get())
	assertFloatEquals(t, 7, rtb.Get())

	rta.Update(params3)
	rtb.Update(params3)
	assertFloatEquals(t, 5, rta.Get())
	assertFloatEquals(t, 5, rtb.Get())
}
