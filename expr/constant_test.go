package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConstant(t *testing.T) {
	e := CONST(5.5)
	params := Map{
		"a": 8.8,
		"b": 4.4,
	}

	assert.Equal(t, []string{}, e.DependsOn())
	b := make([]byte, e.EncodedWidth())
	e.Update(b, params)
	val, _, _ := e.Get(b)
	assertFloatEquals(t, 5.5, val)
}
