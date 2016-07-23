package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCombined(t *testing.T) {
	e, err := JS(`DIV(MULT(AVG("a"), AVG("b")), COUNT("b"))`)
	if !assert.NoError(t, err, "Unable to parse JS expression") {
		return
	}
	params1 := Map{
		"a": 2,
		"b": 10,
	}
	params2 := Map{
		"a": 4,
		"b": 20,
	}
	params3 := Map{
		"a": 0,
		"b": 3,
	}

	assert.Equal(t, []string{"a", "b"}, e.DependsOn())
	b := make([]byte, e.EncodedWidth())
	e.Update(b, params1)
	e.Update(b, params2)
	val, _ := e.Get(b)
	assertFloatEquals(t, 22.5, val)

	b2 := make([]byte, e.EncodedWidth())
	e.Update(b2, params3)
	b3 := make([]byte, e.EncodedWidth())
	e.Merge(b3, b, b2)
	val, _ = e.Get(b3)
	assertFloatEquals(t, 7.33333333, val)
}
