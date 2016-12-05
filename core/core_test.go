package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
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
		Label: "test",
	}

	f.Connect(&goodSource{})
	f.Connect(&goodSource{})
	f.Connect(&errorSource{})

	totalA := int64(0)
	totalB := int64(0)

	err := f.Iterate(Context(), func(dims bytemap.ByteMap, vals Vals) (bool, error) {
		a, _ := vals[0].ValueAt(0, eA)
		b, _ := vals[1].ValueAt(0, eB)
		atomic.AddInt64(&totalA, int64(a))
		atomic.AddInt64(&totalB, int64(b))
		return true, nil
	})

	assert.Equal(t, errTest, err, "Error should have propagated")
	assert.EqualValues(t, 520, atomic.LoadInt64(&totalB), "Total for b should include data from both good sources")
	assert.EqualValues(t, 0, atomic.LoadInt64(&totalA), "Filter should have excluded anything with a value for A")
}

func TestDeadline(t *testing.T) {
	f := &Filter{
		Include: func(dims bytemap.ByteMap, vals Vals) bool {
			// Slow things down by sleeping for a bit
			time.Sleep(100 * time.Millisecond)
			return true
		},
		Label: "deadline",
	}

	f.Connect(&goodSource{})
	f.Connect(&goodSource{})
	f.Connect(&errorSource{})

	rowsSeen := int64(0)

	ctx, cancel := context.WithDeadline(Context(), time.Now().Add(50*time.Millisecond))
	defer cancel()
	err := f.Iterate(ctx, func(dims bytemap.ByteMap, vals Vals) (bool, error) {
		atomic.AddInt64(&rowsSeen, 1)
		return true, nil
	})

	assert.Equal(t, ErrDeadlineExceeded, err, "Should have gotten deadline exceeded error")
	assert.EqualValues(t, 2, atomic.LoadInt64(&rowsSeen), "Should have gotten only two rows (1 from each good source before deadline was exceeded)")
}

func TestGroupSingle(t *testing.T) {
	eTotal := ADD(eA, eB)
	gx := &Group{
		By: []GroupBy{NewGroupBy("x", goexpr.Param("x"))},
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

	t.Log(FormatSource(gx))

	totalByX := make(map[int]float64, 0)
	err := gx.Iterate(Context(), func(key bytemap.ByteMap, vals Vals) (bool, error) {
		total := float64(0)
		v := vals[0]
		for p := 0; p < v.NumPeriods(eTotal.EncodedWidth()); p++ {
			val, _ := v.ValueAt(p, eTotal)
			total += val
		}
		totalByX[key.Get("x").(int)] = total
		return true, nil
	})

	assert.Equal(t, errTest, err, "Error should have propagated")
	assert.EqualValues(t, 240, totalByX[1])
	assert.EqualValues(t, 280, totalByX[2])
}

func TestGroupNone(t *testing.T) {
	eTotal := ADD(eA, eB)
	gx := &Group{
		Fields: Fields{
			Field{
				Name: "total",
				Expr: eTotal,
			},
		},
		Resolution: resolution * 10,
	}

	gx.Connect(&goodSource{})
	gx.Connect(&goodSource{})
	gx.Connect(&errorSource{})

	expectedValues := map[string]float64{
		"1.1": (10 + 70) * 2,
		"1.3": (50) * 2,
		"1.5": (90) * 2,
		"2.2": (100) * 2,
		"2.3": (20 + 80) * 2,
		"2.5": (60) * 2,
	}

	ctx := Context()
	err := gx.Iterate(ctx, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		dims := fmt.Sprintf("%d.%d", key.Get("x"), key.Get("y"))
		val, _ := vals[0].ValueAt(0, eTotal)
		expectedVal := expectedValues[dims]
		delete(expectedValues, dims)
		assert.Equal(t, expectedVal, val, dims)
		return true, nil
	})

	assert.Equal(t, errTest, err, "Error should have propagated")
	assert.Empty(t, expectedValues, "All combinations should have been seen")
	assert.EqualValues(t, []string{"x", "y"}, GetMD(ctx, MDKeyDims))
}

func TestFlattenSortOffsetAndLimit(t *testing.T) {
	f := Flatten()
	f.Connect(&goodSource{})
	f.Connect(&goodSource{})
	f.Connect(&errorSource{})

	s := Sort(NewOrderBy("b", true), NewOrderBy("a", false))
	s.Connect(f)

	o := Offset(1)
	o.Connect(s)

	l := Limit(14)
	l.Connect(o)

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
	err := l.Iterate(Context(), func(row *FlatRow) (bool, error) {
		expectedTS, expectedTSs = expectedTSs[0], expectedTSs[1:]
		expectedA, expectedAs = expectedAs[0], expectedAs[1:]
		expectedB, expectedBs = expectedBs[0], expectedBs[1:]
		assert.Equal(t, expectedTS.UnixNano(), row.TS)
		assert.EqualValues(t, expectedA, row.Values[0])
		assert.EqualValues(t, expectedB, row.Values[1])
		return true, nil
	})

	assert.Equal(t, errTest, err, "Error should have propagated")

	t.Log(FormatSource(l))
}

type testSource struct{}

func (s *testSource) GetFields() Fields {
	return Fields{NewField("a", eA), NewField("b", eB)}
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

func (s *goodSource) Iterate(ctx context.Context, onRow OnRow) error {
	deadline, hasDeadline := ctx.Deadline()
	hitDeadline := func() bool {
		return hasDeadline && time.Now().After(deadline)
	}

	if hitDeadline() {
		return ErrDeadlineExceeded
	}
	onRow(makeRow(epoch.Add(-9*resolution), 1, 1, 10, 0))
	if hitDeadline() {
		return ErrDeadlineExceeded
	}
	onRow(makeRow(epoch.Add(-8*resolution), 2, 3, 0, 20))
	if hitDeadline() {
		return ErrDeadlineExceeded
	}

	// Intentional gap
	onRow(makeRow(epoch.Add(-5*resolution), 1, 3, 50, 0))
	if hitDeadline() {
		return ErrDeadlineExceeded
	}
	onRow(makeRow(epoch.Add(-4*resolution), 2, 5, 0, 60))
	if hitDeadline() {
		return ErrDeadlineExceeded
	}
	onRow(makeRow(epoch.Add(-3*resolution), 1, 1, 70, 0))
	if hitDeadline() {
		return ErrDeadlineExceeded
	}
	onRow(makeRow(epoch.Add(-2*resolution), 2, 3, 0, 80))
	if hitDeadline() {
		return ErrDeadlineExceeded
	}
	onRow(makeRow(epoch.Add(-1*resolution), 1, 5, 90, 0))
	if hitDeadline() {
		return ErrDeadlineExceeded
	}
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

func (s *goodSource) String() string {
	return "test.good"
}

type errorSource struct {
	testSource
}

func (s *errorSource) Iterate(ctx context.Context, onRow OnRow) error {
	return errTest
}

func (s *errorSource) String() string {
	return "test.error"
}
