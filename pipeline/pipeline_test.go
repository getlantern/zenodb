package pipeline

import (
	"errors"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/encoding"
	. "github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
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

	totalA := int64(0)
	totalB := int64(0)

	err := f.Iterate(func(dims bytemap.ByteMap, vals Vals) {
		a, _ := vals[0].ValueAt(0, eA)
		b, _ := vals[1].ValueAt(0, eB)
		atomic.AddInt64(&totalA, int64(a))
		atomic.AddInt64(&totalB, int64(b))
	})

	assert.Equal(t, errTest, err, "Error should have propagated")
	assert.EqualValues(t, 520, atomic.LoadInt64(&totalB), "Total for b should include data from both good sources")
	assert.EqualValues(t, 0, atomic.LoadInt64(&totalA), "Filter should have excluded anything with a value for A")
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
	assert.EqualValues(t, 240, totalByX[1])
	assert.EqualValues(t, 280, totalByX[2])
}

func TestFlattenSortAndLimit(t *testing.T) {
	f := &Flatten{}
	f.Connect(&goodSource{})
	f.Connect(&goodSource{})
	f.Connect(&errorSource{})

	s := &Sort{
		OrderBy: []Order{
			Order{Field: "b", Descending: true},
			Order{Field: "a"},
		},
	}
	s.Connect(f)

	l := &Limit{
		Offset: 1,
		Limit:  14,
	}
	l.Connect(s)

	// This contains the data, sorted, but missing the first and last entries
	expectedTSs := []time.Time{
		epoch, epoch.Add(-2 * resolution), epoch.Add(-2 * resolution), epoch.Add(-4 * resolution), epoch.Add(-4 * resolution), epoch.Add(-8 * resolution), epoch.Add(-8 * resolution),
		epoch.Add(-9 * resolution), epoch.Add(-9 * resolution), epoch.Add(-5 * resolution), epoch.Add(-5 * resolution), epoch.Add(-3 * resolution), epoch.Add(-3 * resolution), epoch.Add(-1 * resolution),
	}
	expectedAs := []float64{0, 0, 0, 0, 0, 0, 0, 10, 10, 50, 50, 70, 70, 90}
	expectedBs := []float64{100, 80, 80, 60, 60, 20, 20, 0, 0, 0, 0, 0, 0, 0}
	var expectedTS time.Time
	var expectedA float64
	var expectedB float64
	err := l.Iterate(func(row *FlatRow) {
		expectedTS, expectedTSs = expectedTSs[0], expectedTSs[1:]
		expectedA, expectedAs = expectedAs[0], expectedAs[1:]
		expectedB, expectedBs = expectedBs[0], expectedBs[1:]
		assert.Equal(t, expectedTS.UnixNano(), row.TS)
		assert.EqualValues(t, expectedA, row.Values[0])
		assert.EqualValues(t, expectedB, row.Values[1])
	})

	assert.Equal(t, errTest, err, "Error should have propagated")
}

type testSource struct{}

func (s *testSource) GetFields() Fields {
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

func (s *testSource) GetResolution() time.Duration {
	return resolution
}

func (s *testSource) GetAsOf() time.Time {
	return asOf
}

func (s *testSource) GetUntil() time.Time {
	return until
}

type goodSource struct {
	testSource
}

func (s *goodSource) Iterate(onRow OnRow) error {
	onRow(makeRow(epoch.Add(-9*resolution), 1, 1, 10, 0))
	onRow(makeRow(epoch.Add(-8*resolution), 2, 3, 0, 20))
	// Intentional gap
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
