package planner

import (
	"context"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/core"
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
)

func TestPlanner(t *testing.T) {
	pairs := map[string]func() core.Source{
		"SELECT * FROM TableA": func() core.Source {
			t := &testTable{"tablea"}
			f := core.Flatten()
			f.Connect(t)
			return f
		},
		"SELECT *, a + b AS total FROM TableA": func() core.Source {
			t := &testTable{"tablea"}
			g := &core.Group{
				Fields: []core.Field{core.NewField("a", eA), core.NewField("b", eB), core.NewField("total", ADD(eA, eB))},
			}
			f := core.Flatten()
			g.Connect(t)
			f.Connect(g)
			return f
		},
	}

	for sqlString, expected := range pairs {
		plan, err := Plan(sqlString, &Opts{
			GetTable: func(table string) core.RowSource {
				return &testTable{table}
			},
			Now: func(table string) time.Time {
				return epoch
			},
			FieldSource: func(table string) ([]core.Field, error) {
				t := &testTable{table}
				return t.GetFields(), nil
			},
		})

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, core.FormatSource(expected()), core.FormatSource(plan))
	}
}

type testTable struct {
	name string
}

func (t *testTable) GetFields() core.Fields {
	return core.Fields{core.NewField("a", eA), core.NewField("b", eB)}
}

func (t *testTable) GetResolution() time.Duration {
	return resolution
}

func (t *testTable) GetAsOf() time.Time {
	return asOf
}

func (t *testTable) GetUntil() time.Time {
	return until
}

func (t *testTable) Iterate(ctx context.Context, onRow core.OnRow) error {
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

func (t *testTable) String() string {
	return t.name
}
