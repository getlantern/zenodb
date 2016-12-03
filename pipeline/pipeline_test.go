package pipeline

import (
	"errors"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/encoding"
	. "github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	epoch      = time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	resolution = 1 * time.Second
	asOf       = epoch.Add(-10 * resolution)
	until      = epoch

	eA = SUM("a")
	eB = SUM("b")

	errTest = errors.New("test error")
)

func TestFilter(t *testing.T) {
	f := &Filter{
		Include: func(dims bytemap.ByteMap, vals Vals) bool {
			x := dims.Get("x")
			return x != nil && x.(int)%2 == 0
		},
	}

	f.Connect(&goodSource{})
	f.Connect(&goodSource{})
	f.Connect(&errorSource{})

	totalA := float64(0)
	totalB := float64(0)

	err := f.Iterate(func(dims bytemap.ByteMap, vals Vals) {
		a, _ := vals[0].ValueAt(0, eA)
		b, _ := vals[1].ValueAt(0, eB)
		totalA += a
		totalB += b
	})

	assert.Equal(t, errTest, err, "Error should have propagated")
	assert.EqualValues(t, 600, totalB, "Total for b should include data from both good sources")
	assert.EqualValues(t, 0, totalA, "Filter should have excluded anything with a value for A")
}

func TestGroup(t *testing.T) {
	eTotal := ADD(eA, eB)
	gx := &Group{
		Dims: []string{"x"},
		Fields: Fields{
			Field{
				Name: "total",
				Expr: eTotal,
			},
		},
		Resolution: resolution * 2,
		AsOf:       asOf.Add(2 * resolution),
		Until:      until.Add(-2 * resolution),
	}

	gx.Connect(&goodSource{})
	gx.Connect(&goodSource{})
	gx.Connect(&errorSource{})

	totalByX := make(map[int]float64, 0)
	err := gx.Iterate(func(dims bytemap.ByteMap, vals Vals) {
		total := float64(0)
		v := vals[0]
		for p := 0; p < v.NumPeriods(eTotal.EncodedWidth()); p++ {
			val, _ := v.ValueAt(p, eTotal)
			total += val
		}
		totalByX[dims.Get("x").(int)] = total
	})

	assert.Equal(t, errTest, err, "Error should have propagated")
	assert.EqualValues(t, 300, totalByX[1])
	assert.EqualValues(t, 360, totalByX[2])
}

type testSource struct{}

func (s *testSource) Fields() Fields {
	return Fields{
		Field{
			Name: "a",
			Expr: eA,
		},
		Field{
			Name: "b",
			Expr: eB,
		},
	}
}

func (s *testSource) Resolution() time.Duration {
	return resolution
}

func (s *testSource) AsOf() time.Time {
	return asOf
}

func (s *testSource) Until() time.Time {
	return until
}

type goodSource struct {
	testSource
}

func (s *goodSource) Iterate(onRow OnRow) error {
	onRow(makeRow(epoch.Add(-9*resolution), 1, 1, 10, 0))
	onRow(makeRow(epoch.Add(-8*resolution), 2, 3, 0, 20))
	onRow(makeRow(epoch.Add(-7*resolution), 1, 5, 30, 0))
	onRow(makeRow(epoch.Add(-6*resolution), 2, 1, 0, 40))
	onRow(makeRow(epoch.Add(-5*resolution), 1, 3, 50, 0))
	onRow(makeRow(epoch.Add(-4*resolution), 2, 5, 0, 60))
	onRow(makeRow(epoch.Add(-3*resolution), 1, 1, 70, 0))
	onRow(makeRow(epoch.Add(-2*resolution), 2, 3, 0, 80))
	onRow(makeRow(epoch.Add(-1*resolution), 1, 5, 90, 0))
	onRow(makeRow(epoch, 2, 2, 0, 100))
	return nil
}

func makeRow(ts time.Time, x int, y int, a float64, b float64) (bytemap.ByteMap, []encoding.Sequence) {
	key := bytemap.New(map[string]interface{}{"x": x, "y": y})
	vals := make([]encoding.Sequence, 2)
	if a != 0 {
		vals[0] = encoding.NewValue(eA, ts, a)
	}
	if b != 0 {
		vals[1] = encoding.NewValue(eB, ts, b)
	}
	return key, vals
}

type errorSource struct {
	testSource
}

func (s *errorSource) Iterate(onRow OnRow) error {
	return errTest
}
