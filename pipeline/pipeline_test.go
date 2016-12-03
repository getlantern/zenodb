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

	eA = SUM("a")
	eB = SUM("b")

	testError = errors.New("test error")
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

	assert.Equal(t, testError, err, "Error should have propagated")
	assert.EqualValues(t, 600, totalB, "Total for b should include data from both good sources")
	assert.EqualValues(t, 0, totalA, "Filter should have excluded anything with a value for A")
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
	return epoch.Add(-10 * resolution)
}

func (s *testSource) Until() time.Time {
	return epoch
}

type goodSource struct {
	testSource
}

func (s *goodSource) Iterate(onRow OnRow) error {
	onRow(makeRow(epoch, 1, 1, 10, 0))
	onRow(makeRow(epoch, 2, 1, 0, 20))
	onRow(makeRow(epoch, 3, 1, 30, 0))
	onRow(makeRow(epoch, 4, 1, 0, 40))
	onRow(makeRow(epoch, 5, 1, 50, 0))
	onRow(makeRow(epoch, 6, 1, 0, 60))
	onRow(makeRow(epoch, 7, 1, 70, 0))
	onRow(makeRow(epoch, 8, 1, 0, 80))
	onRow(makeRow(epoch, 9, 1, 90, 0))
	onRow(makeRow(epoch, 10, 1, 0, 100))
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
	return testError
}
