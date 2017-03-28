package expr

import (
	"testing"
)

func TestConstant(t *testing.T) {
	e := msgpacked(t, CONST(5.5))
	params := Map{
		"a": 8.8,
		"b": 4.4,
	}

	b := make([]byte, e.EncodedWidth())
	e.Update(b, params, nil)
	val, _, _ := e.Get(b)
	assertFloatEquals(t, 5.5, val)
}
