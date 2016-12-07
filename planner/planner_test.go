package planner

import (
	"context"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
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

	fieldA = core.NewField("a", eA)
	fieldB = core.NewField("b", eB)
)

func TestPlanner(t *testing.T) {
	pairs := map[string]func() core.Source{
		"SELECT * FROM TableA": func() core.Source {
			t := &testTable{"tablea"}
			f := core.Flatten()
			f.Connect(t)
			return f
		},
		"SELECT * FROM TableA WHERE x > 5": func() core.Source {
			t := &testTable{"tablea"}
			fi := &core.RowFilter{
				Label: "where x > 5",
			}
			f := core.Flatten()
			fi.Connect(t)
			f.Connect(fi)
			return f
		},
		"SELECT * FROM TableA WHERE dim IN (SELECT DIM FROM tableb)": func() core.Source {
			t := &testTable{"tablea"}
			fi := &core.RowFilter{
				Label: "where dim in (select dim as dim from tableb)",
			}
			f := core.Flatten()
			fi.Connect(t)
			f.Connect(fi)
			return f
		},
		"SELECT * FROM TableA LIMIT 2, 5": func() core.Source {
			t := &testTable{"tablea"}
			f := core.Flatten()
			o := core.Offset(2)
			l := core.Limit(5)
			f.Connect(t)
			o.Connect(f)
			l.Connect(o)
			return l
		},
		"SELECT *, a + b AS total FROM TableA": func() core.Source {
			fieldTotal := core.NewField("total", ADD(eA, eB))
			t := &testTable{"tablea"}
			g := &core.Group{
				Fields: []core.Field{fieldA, fieldB, fieldTotal},
			}
			f := core.Flatten()
			g.Connect(t)
			f.Connect(g)
			return f
		},
		"SELECT * FROM TableA HAVING a+b > 0": func() core.Source {
			fieldHaving := core.NewField("_having", GT(ADD(eA, eB), CONST(0)))
			t := &testTable{"tablea"}
			g := &core.Group{
				Fields: []core.Field{fieldA, fieldB, fieldHaving},
			}
			f := core.Flatten()
			h := &core.FlatRowFilter{
				Label: "a+b > 0",
			}
			g.Connect(t)
			f.Connect(g)
			h.Connect(f)
			return h
		},
		"SELECT * FROM TableA ASOF '-5s'": func() core.Source {
			t := &testTable{"tablea"}
			g := &core.Group{
				Fields: []core.Field{core.NewField("a", eA), core.NewField("b", eB)},
				AsOf:   epoch.Add(-5 * time.Second),
			}
			f := core.Flatten()
			g.Connect(t)
			f.Connect(g)
			return f
		},
		"SELECT * FROM TableA ASOF '-5s' UNTIL '-1s'": func() core.Source {
			t := &testTable{"tablea"}
			g := &core.Group{
				Fields: []core.Field{core.NewField("a", eA), core.NewField("b", eB)},
				AsOf:   epoch.Add(-5 * time.Second),
				Until:  epoch.Add(-1 * time.Second),
			}
			f := core.Flatten()
			g.Connect(t)
			f.Connect(g)
			return f
		},
		"SELECT * FROM TableA GROUP BY period(2s)": func() core.Source {
			t := &testTable{"tablea"}
			g := &core.Group{
				Fields:     []core.Field{core.NewField("a", eA), core.NewField("b", eB)},
				Resolution: 2 * time.Second,
			}
			f := core.Flatten()
			g.Connect(t)
			f.Connect(g)
			return f
		},
		"SELECT *, a + b AS total FROM TableA ASOF '-5s' UNTIL '-1s' WHERE x > 5 GROUP BY y, period(2s) ORDER BY total DESC LIMIT 2, 5": func() core.Source {
			t := &testTable{"tablea"}
			fi := &core.RowFilter{
				Label: "where x > 5",
			}
			g := &core.Group{
				By:         []core.GroupBy{core.NewGroupBy("y", goexpr.Param("y"))},
				Fields:     core.Fields{core.NewField("a", eA), core.NewField("b", eB), core.NewField("total", ADD(eA, eB))},
				AsOf:       epoch.Add(-5 * time.Second),
				Until:      epoch.Add(-1 * time.Second),
				Resolution: 2 * time.Second,
			}
			f := core.Flatten()
			s := core.Sort(core.NewOrderBy("total", true))
			o := core.Offset(2)
			l := core.Limit(5)
			fi.Connect(t)
			g.Connect(fi)
			f.Connect(g)
			s.Connect(f)
			o.Connect(s)
			l.Connect(o)
			return l
		},
		"SELECT * FROM (SELECT * FROM TableA)": func() core.Source {
			t := &testTable{"tablea"}
			f := core.Flatten()
			u := core.Unflatten()
			f2 := core.Flatten()
			f.Connect(t)
			u.Connect(f)
			f2.Connect(u)
			return f2
		},
		"SELECT AVG(a) + AVG(b) AS total, * FROM (SELECT * FROM TableA)": func() core.Source {
			avgTotal := core.NewField("total", ADD(AVG("a"), AVG("b")))
			t := &testTable{"tablea"}
			f := core.Flatten()
			u := core.Unflatten(avgTotal, fieldA, fieldB)
			g := &core.Group{
				Fields: core.Fields{avgTotal, fieldA, fieldB},
			}
			f2 := core.Flatten()
			f.Connect(t)
			u.Connect(f)
			g.Connect(u)
			f2.Connect(g)
			return f2
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

		assert.Equal(t, core.FormatSource(expected()), core.FormatSource(plan), sqlString)
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
		vals[0] = encoding.NewFloatValue(eA, ts, a)
	}
	if b != 0 {
		vals[1] = encoding.NewFloatValue(eB, ts, b)
	}
	return key, vals
}

func (t *testTable) String() string {
	return t.name
}
