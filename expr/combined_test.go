package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCombined(t *testing.T) {
	mult := MULT(AVG("a"), AVG("b"))
	count := COUNT("b")
	e := DIV(mult, count)
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
	val, _, _ := e.Get(b)
	assertFloatEquals(t, 22.5, val)

	b2 := make([]byte, e.EncodedWidth())
	e.Update(b2, params3)
	b3 := make([]byte, e.EncodedWidth())
	e.Merge(b3, b, b2)
	val, _, _ = e.Get(b3)
	assertFloatEquals(t, 7.33333333, val)

	// Test SubMerge
	bmult := make([]byte, mult.EncodedWidth())
	bcount := make([]byte, count.EncodedWidth())
	mult.Update(bmult, params1)
	count.Update(bcount, params1)
	mult.Update(bmult, params2)
	count.Update(bcount, params2)
	be := make([]byte, e.EncodedWidth())
	EnsureSubMerge(e.SubMerger(mult))(be, bmult)
	EnsureSubMerge(e.SubMerger(count))(be, bcount)
	val, _, _ = e.Get(b)
	assertFloatEquals(t, 22.5, val)

	// Test noop submerge
	assert.Nil(t, mult.SubMerger(SUM("unknown")))
}
