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

	cond, _    = goexpr.Boolean(">", goexpr.Param("d"), goexpr.Constant(0))
	eA         = IF(cond, SUM("a"))
	eB         = SUM("b")
	totalField = NewField("total", ADD(eA, eB))

	errTest = errors.New("test error")
)

func TestRowFilter(t *testing.T) {
	f := RowFilter(&goodSource{}, "test", func(ctx context.Context, key bytemap.ByteMap, fields Fields, vals Vals) (bytemap.ByteMap, Vals, error) {
		x := key.Get("x")
		if x != nil && x.(int)%2 == 0 {
			return key, vals, nil
		}
		return nil, nil, nil
	})

	totalA := int64(0)
	totalB := int64(0)

	err := f.Iterate(context.Background(), FieldsIgnored, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		a, _ := vals[0].ValueAt(0, eA)
		b, _ := vals[1].ValueAt(0, eB)
		atomic.AddInt64(&totalA, int64(a))
		atomic.AddInt64(&totalB, int64(b))
		return true, nil
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 260, atomic.LoadInt64(&totalB))
	assert.EqualValues(t, 0, atomic.LoadInt64(&totalA), "Filter should have excluded anything with a value for A")
}

func TestFlatRowFilter(t *testing.T) {
	f := FlatRowFilter(Flatten(&goodSource{}), "test", func(ctx context.Context, row *FlatRow, fields Fields) (*FlatRow, error) {
		x := row.Key.Get("x")
		if x != nil && x.(int)%2 == 0 {
			return row, nil
		}
		return nil, nil
	})

	totalA := int64(0)
	totalB := int64(0)

	err := f.Iterate(context.Background(), FieldsIgnored, func(row *FlatRow) (bool, error) {
		a := row.Values[0]
		b := row.Values[1]
		atomic.AddInt64(&totalA, int64(a))
		atomic.AddInt64(&totalB, int64(b))
		return true, nil
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 260, atomic.LoadInt64(&totalB))
	assert.EqualValues(t, 0, atomic.LoadInt64(&totalA), "Filter should have excluded anything with a value for A")
}

func TestDeadlineFilter(t *testing.T) {
	f := RowFilter(&goodSource{}, "deadline", func(ctx context.Context, key bytemap.ByteMap, fields Fields, vals Vals) (bytemap.ByteMap, Vals, error) {
		// Slow things down by sleeping for a bit
		time.Sleep(100 * time.Millisecond)
		return key, vals, nil
	})

	rowsSeen := int64(0)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()
	err := f.Iterate(ctx, FieldsIgnored, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		atomic.AddInt64(&rowsSeen, 1)
		return true, nil
	})

	assert.Equal(t, ErrDeadlineExceeded, err, "Should have gotten deadline exceeded error")
	assert.EqualValues(t, 1, atomic.LoadInt64(&rowsSeen), "Should have gotten only 1 row before deadline exceeded")
}

func TestDeadlineGroup(t *testing.T) {
	eTotal := ADD(eA, eB)
	g := Group(&infiniteSource{}, GroupOpts{
		By: []GroupBy{NewGroupBy("x", goexpr.Param("x"))},
		Fields: StaticFieldSource{
			Field{
				Name: "total",
				Expr: eTotal,
			},
		},
		Resolution: resolution * 2,
		AsOf:       asOf.Add(2 * resolution),
		Until:      until.Add(-2 * resolution),
	})

	rowsSeen := int64(0)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(25*time.Millisecond))
	defer cancel()
	err := g.Iterate(ctx, FieldsIgnored, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		atomic.AddInt64(&rowsSeen, 1)
		return true, nil
	})

	assert.Equal(t, ErrDeadlineExceeded, err, "Should have gotten deadline exceeded error")
	assert.EqualValues(t, 0, atomic.LoadInt64(&rowsSeen), "Should have gotten 0 rows before deadline exceeded")
}

func TestGroupSingle(t *testing.T) {
	eTotal := ADD(eA, eB)
	gx := Group(&goodSource{}, GroupOpts{
		By: []GroupBy{NewGroupBy("x", goexpr.Param("x"))},
		Fields: StaticFieldSource{
			Field{
				Name: "total",
				Expr: eTotal,
			},
		},
		Resolution: resolution * 2,
		AsOf:       asOf.Add(2 * resolution),
		Until:      until.Add(-2 * resolution),
	})

	totalByX := make(map[int]float64, 0)
	var fields Fields
	err := gx.Iterate(context.Background(), func(inFields Fields) error {
		fields = inFields
		return nil
	}, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		t.Log(key.AsMap())
		for i, field := range fields {
			t.Log(vals[i].String(field.Expr, resolution*2))
		}
		total := float64(0)
		v := vals[0]
		for p := 0; p < v.NumPeriods(eTotal.EncodedWidth()); p++ {
			val, _ := v.ValueAt(p, eTotal)
			total += val
		}
		totalByX[key.Get("x").(int)] = total
		return true, nil
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 120, totalByX[1])
	assert.EqualValues(t, 140, totalByX[2])
}

func TestGroupCrosstabSingle(t *testing.T) {
	eAdd := ADD(eA, eB)
	addField := Field{
		Name: "add",
		Expr: eAdd,
	}

	expectedFields := Fields{}
	for _, i := range []string{"1", "2", "3", "5"} {
		cond, err := goexpr.Binary("=", goexpr.Concat(goexpr.Constant("_"), goexpr.Param("y")), goexpr.Constant(i))
		if !assert.NoError(t, err) {
			return
		}
		ifex := IF(cond, eAdd)
		expectedFields = append(expectedFields, NewField(i+"_add", ifex))
	}
	expectedFields = append(expectedFields, NewField("total_add", eAdd))

	expectedKeys := []bytemap.ByteMap{
		bytemap.FromSortedKeysAndValues([]string{"x"}, []interface{}{1}),
		bytemap.FromSortedKeysAndValues([]string{"x"}, []interface{}{2}),
	}

	expectedRows := [][][]float64{
		[][]float64{
			[]float64{70, 0},
			[]float64{0, 0},
			[]float64{0, 50},
			[]float64{0, 0},
			[]float64{70, 50},
		},
		[][]float64{
			[]float64{0, 0},
			[]float64{0, 0},
			[]float64{80, 0},
			[]float64{0, 60},
			[]float64{80, 60},
		},
	}

	gx := Group(&goodSource{}, GroupOpts{
		By:                    []GroupBy{NewGroupBy("x", goexpr.Param("x"))},
		Crosstab:              goexpr.Concat(goexpr.Constant("_"), goexpr.Param("y")),
		CrosstabIncludesTotal: true,
		Fields:                StaticFieldSource{addField},
		Resolution:            resolution * 2,
		AsOf:                  asOf.Add(2 * resolution),
		Until:                 until.Add(-2 * resolution),
	})

	var fields Fields
	err := gx.Iterate(context.Background(), func(inFields Fields) error {
		fields = inFields
		if assert.Equal(t, len(expectedFields), len(fields)) {
			for i, expected := range expectedFields {
				assert.Equal(t, expected.String(), fields[i].String())
			}
		}
		return nil
	}, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		expectedKey := expectedKeys[0]
		expectedKeys = expectedKeys[1:]
		expectedRow := expectedRows[0]
		expectedRows = expectedRows[1:]
		assert.EqualValues(t, expectedKey, key)
		if assert.Equal(t, len(expectedRow), len(vals)) {
			for i, expected := range expectedRow {
				val := vals[i]
				field := fields[i]
				if assert.Equal(t, len(expected), val.NumPeriods(field.Expr.EncodedWidth())) {
					for j, f := range expected {
						actual, _ := val.ValueAt(j, field.Expr)
						assert.Equal(t, f, actual)
					}
				}
			}
		}
		return true, nil
	})

	if !assert.NoError(t, err) {
		t.Log(FormatSource(gx))
	}
}

func TestGroupResolutionOnly(t *testing.T) {
	eTotal := ADD(eA, eB)
	gx := Group(&goodSource{}, GroupOpts{
		By: []GroupBy{NewGroupBy("_", goexpr.Constant("_"))},
		Fields: StaticFieldSource{
			Field{
				Name: "total",
				Expr: eTotal,
			},
		},
		Resolution: resolution * 20,
	})

	total := float64(0)
	err := gx.Iterate(context.Background(), FieldsIgnored, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		v := vals[0]
		for p := 0; p < v.NumPeriods(eTotal.EncodedWidth()); p++ {
			val, _ := v.ValueAt(p, eTotal)
			t.Log(val)
			total += val
		}
		// We expect only one row
		return false, nil
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 480, total)
}

func TestGroupNone(t *testing.T) {
	eTotal := ADD(eA, eB)
	gx := Group(&goodSource{}, GroupOpts{
		Fields: StaticFieldSource{
			Field{
				Name: "total",
				Expr: eTotal,
			},
		},
		Resolution: resolution * 10,
	})

	expectedValues := map[string]float64{
		"1.1": 10 + 70,
		"1.3": 50,
		"1.5": 90,
		"2.2": 100,
		"2.3": 20 + 80,
		"2.5": 60,
	}

	ctx := context.Background()
	err := gx.Iterate(ctx, FieldsIgnored, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		dims := fmt.Sprintf("%d.%d", key.Get("x"), key.Get("y"))
		val, _ := vals[0].ValueAt(0, eTotal)
		expectedVal := expectedValues[dims]
		delete(expectedValues, dims)
		assert.Equal(t, expectedVal, val, dims)
		return true, nil
	})

	assert.NoError(t, err)
	assert.Empty(t, expectedValues, "All combinations should have been seen")
}

func TestFlattenSortOffsetAndLimit(t *testing.T) {
	// TODO: add test that tests flattening of rows that contain multiple periods
	// worth of values
	g := Group(&goodSource{}, GroupOpts{
		Fields: StaticFieldSource{NewField("a", eA), NewField("b", eB), NewField("c", CONST(10))},
	})
	f := Flatten(g)
	s := Sort(f, NewOrderBy("b", true), NewOrderBy("a", false))
	o := Offset(s, 1)
	l := Limit(o, 6)

	// This contains the data, sorted, but missing the first and last entries
	expectedTSs := []time.Time{
		epoch.Add(-2 * resolution), epoch.Add(-4 * resolution), epoch.Add(-8 * resolution),
		epoch.Add(-9 * resolution), epoch.Add(-5 * resolution), epoch.Add(-3 * resolution),
	}
	expectedAs := []float64{0, 0, 0, 10, 50, 70}
	expectedBs := []float64{80, 60, 20, 0, 0, 0}
	var expectedTS time.Time
	var expectedA float64
	var expectedB float64
	err := l.Iterate(context.Background(), FieldsIgnored, func(row *FlatRow) (bool, error) {
		expectedTS, expectedTSs = expectedTSs[0], expectedTSs[1:]
		expectedA, expectedAs = expectedAs[0], expectedAs[1:]
		expectedB, expectedBs = expectedBs[0], expectedBs[1:]
		assert.Equal(t, expectedTS.UnixNano(), row.TS)
		assert.EqualValues(t, expectedA, row.Values[0])
		assert.EqualValues(t, expectedB, row.Values[1])
		return true, nil
	})

	if !assert.NoError(t, err) {
		t.Log(FormatSource(l))
	}
}

func TestUnflattenTransform(t *testing.T) {
	avgTotal := ADD(AVG("a"), AVG("b"))
	f := Flatten(&goodSource{})
	u := Unflatten(f, StaticFieldSource{NewField("total", avgTotal)})
	doTestUnflattened(t, u, avgTotal)
}

func TestUnflattenOptimized(t *testing.T) {
	total := ADD(eA, eB)
	s := &totalingSource{}
	f := Flatten(s)
	u := UnflattenOptimized(f)
	_, isTotalingSource := u.(*totalingSource)

	if !assert.True(t, isTotalingSource, "Unflattened should point directly at totaling source, put points at: %v", u.String()) {
		return
	}

	doTestUnflattened(t, u, total)
}

func doTestUnflattened(t *testing.T, u RowSource, ex Expr) {
	expectedRows := make([]*testRow, 0, len(testRows))
	for _, row := range testRows {
		var ts time.Time
		if row.vals[0] != nil {
			ts = row.vals[0].Until()
		} else {
			ts = row.vals[1].Until()
		}
		a, _ := row.vals[0].ValueAt(0, eA)
		b, _ := row.vals[1].ValueAt(0, eB)
		params := Map(map[string]float64{
			"a": a,
			"b": b,
		})
		expectedRow := &testRow{
			key:  row.key,
			vals: []encoding.Sequence{encoding.NewValue(ex, ts, params, row.key)},
		}
		expectedRows = append(expectedRows, expectedRow)
	}

	err := u.Iterate(context.Background(), FieldsIgnored, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		row := &testRow{key, vals}
		for i, expected := range expectedRows {
			if row.equals(expected) {
				expectedRows = append(expectedRows[:i], expectedRows[i+1:]...)
				break
			}
		}
		return true, nil
	})

	assert.NoError(t, err)
	assert.Empty(t, expectedRows, "All rows should have been seen")
}

type testSource struct{}

func (s *testSource) getFields() Fields {
	return Fields{NewField("a", eA), NewField("b", eB)}
}

func (s *testSource) GetGroupBy() []GroupBy {
	x, _ := goexpr.Binary("+", goexpr.Param("x"), goexpr.Constant(1))
	return []GroupBy{NewGroupBy("x", x), NewGroupBy("y", goexpr.Param("y"))}
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

func (s *testSource) GetCurrentThrough() time.Time {
	return until
}

type testRow struct {
	key  bytemap.ByteMap
	vals Vals
}

func (r *testRow) equals(other *testRow) bool {
	if r == nil || other == nil {
		return false
	}
	if string(r.key) != string(other.key) {
		return false
	}
	if len(r.vals) != len(other.vals) {
		return false
	}
	for i, val := range r.vals {
		ex := eA
		if i > 0 {
			ex = eB
		}
		otherVal := other.vals[i]
		v, _ := val.ValueAt(0, ex)
		ov, _ := otherVal.ValueAt(0, ex)
		if v != ov {
			return false
		}
	}
	return true
}

var testRows = []*testRow{
	makeRow(epoch.Add(-9*resolution), 1, 1, 10, 0),
	makeRow(epoch.Add(-8*resolution), 2, 3, 0, 20),
	// Intentional gap
	makeRow(epoch.Add(-5*resolution), 1, 3, 50, 0),
	makeRow(epoch.Add(-4*resolution), 2, 5, 0, 60),
	makeRow(epoch.Add(-3*resolution), 1, 1, 70, 0),
	makeRow(epoch.Add(-2*resolution), 2, 3, 0, 80),
	makeRow(epoch.Add(-1*resolution), 1, 5, 90, 0),
	makeRow(epoch, 2, 2, 0, 100),
}

func makeRow(ts time.Time, x int, y int, a float64, b float64) *testRow {
	key := bytemap.New(map[string]interface{}{"x": x, "y": y})
	vals := make([]encoding.Sequence, 2)
	if a != 0 {
		vals[0] = encoding.NewFloatValue(eA, ts, a)
	}
	if b != 0 {
		vals[1] = encoding.NewFloatValue(eB, ts, b)
	}
	return &testRow{key, vals}
}

type goodSource struct {
	testSource
}

func (s *goodSource) Iterate(ctx context.Context, onFields OnFields, onRow OnRow) error {
	onFields(s.getFields())

	guard := Guard(ctx)
	for _, row := range testRows {
		if guard.TimedOut() {
			return ErrDeadlineExceeded
		}
		more, err := onRow(row.key, row.vals)
		if !more || err != nil {
			return err
		}
	}

	return nil
}

func (s *goodSource) String() string {
	return "test.good"
}

type infiniteSource struct {
	testSource
}

func (s *infiniteSource) Iterate(ctx context.Context, onFields OnFields, onRow OnRow) error {
	onFields(s.getFields())

	for {
		for _, row := range testRows {
			more, err := onRow(row.key, row.vals)
			if !more || err != nil {
				return err
			}
		}
	}
}

func (s *infiniteSource) String() string {
	return "test.infinite"
}

type errorSource struct {
	testSource
}

func (s *errorSource) Iterate(ctx context.Context, onFields OnFields, onRow OnRow) error {
	return errTest
}

func (s *errorSource) String() string {
	return "test.error"
}

type totalingSource struct {
	goodSource
}

func (s *totalingSource) getFields() Fields {
	return Fields{totalField}
}

func (s *totalingSource) Iterate(ctx context.Context, onFields OnFields, onRow OnRow) error {
	return s.goodSource.Iterate(ctx, onFields, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		a, _ := vals[0].ValueAt(0, eA)
		b, _ := vals[0].ValueAt(0, eB)
		val := encoding.NewValue(totalField.Expr, vals[0].Until(), Map{"a": a, "b": b}, key)
		return onRow(key, Vals{val})
	})
}
