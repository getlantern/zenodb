package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConstant(t *testing.T) {
	e := Constant(5.5)
	params := Map{
		"a": Float(8.8),
		"b": Float(4.4),
	}

	assert.Equal(t, []string{}, e.DependsOn())
	a := e.Accumulator()
	a.Update(params)
	assertFloatEquals(t, 5.5, a.Get())
}
