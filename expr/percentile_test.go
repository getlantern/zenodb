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

func TestPercentile(t *testing.T) {
	exprs := append(make([]Expr, 0), msgpacked(t, PERCENTILE("a", 99, 0, 100, 1)))
	exprs = append(exprs, PERCENTILE(exprs[0], 50, 0, 100, 1))
	exprs = append(exprs, PERCENTILE(exprs[1], 1, 0, 100, 1))
	expecteds := []float64{99, 51, 1}

	if !assert.True(t, IsPercentile(exprs[0])) {
		return
	}
	if !assert.IsType(t, &ptile{}, exprs[0]) {
		return
	}
	if !assert.IsType(t, &ptileOptimized{}, exprs[1]) {
		return
	}
	if !assert.IsType(t, &ptileOptimized{}, exprs[2]) {
		return
	}

	checkValue := func(e Expr, b []byte, expected float64) {
		val, wasSet, _ := e.Get(b)
		if assert.True(t, wasSet) {
			AssertFloatWithin(t, 0.01, expected, val, "Incorrect percentile")
		}
	}

	md := goexpr.MapParams{}

	mergeds := [][]byte{
		make([]byte, exprs[0].EncodedWidth()),
		make([]byte, exprs[0].EncodedWidth()),
		make([]byte, exprs[0].EncodedWidth()),
	}
	bs := [][]byte{
		make([]byte, exprs[0].EncodedWidth()),
		make([]byte, exprs[0].EncodedWidth()),
		make([]byte, exprs[0].EncodedWidth()),
	}

	for i := 0; i < 2; i++ {
		for j := 0; j < 50; j++ {
			// Do some direct updates
			for k := float64(1); k <= 50; k++ {
				for x, e := range exprs {
					e.Update(bs[x], Map{"a": k}, md)
				}
			}

			// Do some point merges
			for k := float64(51); k <= 100; k++ {
				for x, e := range exprs {
					b2 := make([]byte, e.EncodedWidth())
					e.Update(b2, Map{"a": k}, md)
					e.Merge(bs[x], bs[x], b2)
				}
			}
		}

		for i, e := range exprs {
			checkValue(e, bs[i], expecteds[i])
			e.Merge(mergeds[i], mergeds[i], bs[i])
		}
	}

	for i, e := range exprs {
		checkValue(e, mergeds[i], expecteds[i])
	}
}
