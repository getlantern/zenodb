package expr

import (
	"testing"

	"github.com/getlantern/goexpr"
	"github.com/stretchr/testify/assert"
)

func TestScaleInt(t *testing.T) {
	v := 5.126
	AssertFloatEquals(t, v, scaleFromInt(scaleToInt(v, 3), 3))
	AssertFloatEquals(t, 5.12, scaleFromInt(scaleToInt(v, 2), 2))
}

func TestDeAggregatePercentile(t *testing.T) {
	e := msgpacked(t, PERCENTILE("p", 99, 0, 100, 1))
	assert.Equal(t, FIELD("p").String(), e.DeAggregate().String())
}

func TestPercentile(t *testing.T) {
	e := msgpacked(t, PERCENTILE(SUM("a"), 99, 0, 100, 1))
	expected := float64(99)

	eo := msgpacked(t, PERCENTILE(e, 50, 0, 100, 1))
	expectedO := float64(51)

	eo2 := msgpacked(t, PERCENTILEOPT(eo, 1))
	expectedO2 := float64(1)

	if !assert.True(t, IsPercentile(e)) {
		return
	}
	if !assert.IsType(t, &ptile{}, e) {
		return
	}
	if !assert.IsType(t, &ptileOptimized{}, eo) {
		return
	}
	if !assert.IsType(t, &ptileOptimized{}, eo2) {
		return
	}

	checkValue := func(e Expr, b []byte, expected float64) {
		val, wasSet, _ := e.Get(b)
		if assert.True(t, wasSet) {
			AssertFloatWithin(t, 0.01, expected, val, "Incorrect percentile")
		}
	}

	md := goexpr.MapParams{}

	merged := make([]byte, e.EncodedWidth())
	for i := 0; i < 2; i++ {
		b := make([]byte, e.EncodedWidth())
		for j := 0; j < 50; j++ {
			// Do some direct updates
			for k := float64(1); k <= 50; k++ {
				e.Update(b, Map{"a": k}, md)
				// Also update the wrapped expressions to make sure this is a noop
				eo.Update(b, Map{"a": k}, md)
				eo2.Update(b, Map{"a": k}, md)
			}

			// Do some point merges
			for k := float64(51); k <= 100; k++ {
				b2 := make([]byte, e.EncodedWidth())
				e.Update(b2, Map{"a": k}, md)
				e.Merge(b, b, b2)
			}
		}
		checkValue(e, b, expected)
		checkValue(eo, b, expectedO)
		checkValue(eo2, b, expectedO2)
		e.Merge(merged, merged, b)
	}

	checkValue(e, merged, expected)
	checkValue(eo, merged, expectedO)
	checkValue(eo2, merged, expectedO2)
}
