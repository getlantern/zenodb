package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSum(t *testing.T) {
	doTestAggregate(t, Sum("a"), 13.2)
}

func TestMin(t *testing.T) {
	doTestAggregate(t, Min("a"), 4.4)
}

func TestMax(t *testing.T) {
	doTestAggregate(t, Max("a"), 8.8)
}

func TestAvg(t *testing.T) {
	doTestAggregate(t, Avg("a"), 6.6)
}

func TestCount(t *testing.T) {
	doTestAggregate(t, Count("a"), 2)
}

func doTestAggregate(t *testing.T, e Expr, expected float64) {
	params1 := Map{
		"a": Float(4.4),
	}
	params2 := Map{
		"a": Float(8.8),
	}

	assert.Equal(t, []string{"a"}, e.DependsOn())
	a := e.Accumulator()
	a.Update(params1)
	a.Update(params2)
	assertFloatEquals(t, expected, a.Get())
}
