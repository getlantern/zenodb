package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCombined(t *testing.T) {
	e := Avg(Sub(Add(Div("a", "b"), 1), 0.5))
	params1 := Map{
		"a": Float(8.8),
		"b": Float(4.4),
	}
	params2 := Map{
		"a": Float(20),
		"b": Float(5),
	}

	assert.Equal(t, []string{"a", "b"}, e.DependsOn())
	a := e.Accumulator()
	a.Update(params1)
	a.Update(params2)
	assertFloatEquals(t, 3.5, a.Get())
}
