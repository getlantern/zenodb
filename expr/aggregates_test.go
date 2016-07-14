package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSUM(t *testing.T) {
	doTestAggregate(t, SUM("a"), []string{"a"}, 13.2)
}

func TestMIN(t *testing.T) {
	doTestAggregate(t, MIN("a"), []string{"a"}, 4.4)
}

func TestMAX(t *testing.T) {
	doTestAggregate(t, MAX("a"), []string{"a"}, 8.8)
}

func TestAVG(t *testing.T) {
	doTestAggregate(t, AVG("a"), []string{"a"}, 6.6)
}

func TestCOUNT(t *testing.T) {
	doTestAggregate(t, COUNT("b"), []string{"b"}, 2)
}

func doTestAggregate(t *testing.T, e Expr, expectedDepends []string, expected float64) {
	params1 := Map{
		"a": 4.4,
	}
	params2 := Map{
		"a": 8.8,
		"b": 1.1,
	}

	assert.Equal(t, expectedDepends, e.DependsOn())
	a := e.Accumulator()
	a.Update(params1)
	a.Update(params2)
	assertFloatEquals(t, expected, a.Get())

	rt := e.Accumulator()
	rt.InitFrom(Encoded(a))
	assertFloatEquals(t, expected, rt.Get())
}
