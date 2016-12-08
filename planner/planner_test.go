package planner

import (
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
	// "github.com/getlantern/goexpr"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	. "github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/sql"
	"github.com/stretchr/testify/assert"
	"strings"
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

func TestPlans(t *testing.T) {
	var descriptions []string
	var queries [][]string
	var expected []func() core.Source
	var expectedCluster []func() core.Source

	multiScenario := func(desc string, sqlStrings []string, sourceFn func() core.Source, clusterSourceFn func() core.Source) {
		descriptions = append(descriptions, desc)
		queries = append(queries, sqlStrings)
		expected = append(expected, sourceFn)
		expectedCluster = append(expectedCluster, clusterSourceFn)
	}

	scenario := func(desc string, sqlString string, sourceFn func() core.Source, clusterSourceFn func() core.Source) {
		multiScenario(desc, []string{sqlString}, sourceFn, clusterSourceFn)
	}

	scenario("No grouping", "SELECT * FROM TableA", func() core.Source {
		t := &testTable{"tablea"}
		f := core.Flatten()
		f.Connect(t)
		return f
	}, func() core.Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea"},
		}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB},
		}
		uf := core.Unflatten(g.Fields...)
		f := core.Flatten()
		uf.Connect(t)
		g.Connect(uf)
		f.Connect(g)
		return f
	})

	multiScenario("Multi select", []string{"SELECT * FROM TableA", "SELECT * FROM TableA"}, func() core.Source {
		t := &testTable{"tablea"}
		s := core.NewSplitter()
		s.Connect(t)
		m := core.MergeFlat()
		for i := 0; i < 2; i++ {
			f := core.Flatten()
			f.Connect(s.Split())
			m.Connect(f)
		}
		return m
	}, nil)

	scenario("WHERE clause", "SELECT * FROM TableA WHERE x > 5", func() core.Source {
		t := &testTable{"tablea"}
		fi := &core.RowFilter{
			Label: "where x > 5",
		}
		f := core.Flatten()
		fi.Connect(t)
		f.Connect(fi)
		return f
	}, func() core.Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea where x > 5"},
		}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB},
		}
		uf := core.Unflatten(g.Fields...)
		f := core.Flatten()
		uf.Connect(t)
		g.Connect(uf)
		f.Connect(g)
		return f
	})

	scenario("WHERE with subquery", "SELECT * FROM TableA WHERE dim IN (SELECT DIM FROM tableb)", func() core.Source {
		t := &testTable{"tablea"}
		fi := &core.RowFilter{
			Label: "where dim in (select dim as dim from tableb)",
		}
		f := core.Flatten()
		fi.Connect(t)
		f.Connect(fi)
		return f
	}, func() core.Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea where dim in (select dim from tableb)"},
		}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB},
		}
		uf := core.Unflatten(g.Fields...)
		f := core.Flatten()
		uf.Connect(t)
		g.Connect(uf)
		f.Connect(g)
		return f
	})

	scenario("LIMIT and OFFSET", "SELECT * FROM TableA LIMIT 2, 5", func() core.Source {
		t := &testTable{"tablea"}
		f := core.Flatten()
		o := core.Offset(2)
		l := core.Limit(5)
		f.Connect(t)
		o.Connect(f)
		l.Connect(o)
		return l
	}, func() core.Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea"},
		}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB},
		}
		uf := core.Unflatten(g.Fields...)
		f := core.Flatten()
		o := core.Offset(2)
		l := core.Limit(5)
		uf.Connect(t)
		g.Connect(uf)
		f.Connect(g)
		o.Connect(f)
		l.Connect(o)
		return l
	})

	scenario("Calculated field", "SELECT *, a + b AS total FROM TableA", func() core.Source {
		fieldTotal := core.NewField("total", ADD(eA, eB))
		t := &testTable{"tablea"}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB, fieldTotal},
		}
		f := core.Flatten()
		g.Connect(t)
		f.Connect(g)
		return f
	}, func() core.Source {
		fieldTotal := core.NewField("total", ADD(eA, eB))
		t := &clusterSource{
			query: &sql.Query{SQL: "select *, a+b as total from tablea"},
		}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB, fieldTotal},
		}
		uf := core.Unflatten(g.Fields...)
		f := core.Flatten()
		uf.Connect(t)
		g.Connect(uf)
		f.Connect(g)
		return f
	})

	scenario("HAVING clause", "SELECT * FROM TableA HAVING a+b > 0", func() core.Source {
		fieldHaving := core.NewField("_having", GT(ADD(eA, eB), CONST(0)))
		t := &testTable{"tablea"}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB, fieldHaving},
		}
		f := core.Flatten()
		h := &core.FlatRowFilter{
			Label: "a+b > 0",
		}
		g.Connect(t)
		f.Connect(g)
		h.Connect(f)
		return h
	}, func() core.Source {
		fieldHaving := core.NewField("_having", GT(ADD(eA, eB), CONST(0)))
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea"},
		}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB, fieldHaving},
		}
		uf := core.Unflatten(g.Fields...)
		f := core.Flatten()
		uf.Connect(t)
		g.Connect(uf)
		f.Connect(g)
		h := &core.FlatRowFilter{
			Label: "a+b > 0",
		}
		h.Connect(f)
		return h
	})

	scenario("HAVING clause with single group by, pushdown allowed", "SELECT * FROM TableA GROUP BY x HAVING a+b > 0", func() core.Source {
		fieldHaving := core.NewField("_having", GT(ADD(eA, eB), CONST(0)))
		t := &testTable{"tablea"}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB, fieldHaving},
			By:     []core.GroupBy{core.NewGroupBy("x", goexpr.Param("x"))},
		}
		f := core.Flatten()
		h := &core.FlatRowFilter{
			Label: "a+b > 0",
		}
		g.Connect(t)
		f.Connect(g)
		h.Connect(f)
		return h
	}, func() core.Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from TableA group by x having a+b > 0"},
		}
		return t
	})

	scenario("ASOF", "SELECT * FROM TableA ASOF '-5s'", func() core.Source {
		t := &testTable{"tablea"}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, core.NewField("a", eA), core.NewField("b", eB)},
			AsOf:   epoch.Add(-5 * time.Second),
		}
		f := core.Flatten()
		g.Connect(t)
		f.Connect(g)
		return f
	}, func() core.Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea ASOF '-5s'"},
		}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB},
		}
		uf := core.Unflatten(g.Fields...)
		f := core.Flatten()
		uf.Connect(t)
		g.Connect(uf)
		f.Connect(g)
		return f
	})

	scenario("ASOF UNTIL", "SELECT * FROM TableA ASOF '-5s' UNTIL '-1s'", func() core.Source {
		t := &testTable{"tablea"}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, core.NewField("a", eA), core.NewField("b", eB)},
			AsOf:   epoch.Add(-5 * time.Second),
			Until:  epoch.Add(-1 * time.Second),
		}
		f := core.Flatten()
		g.Connect(t)
		f.Connect(g)
		return f
	}, func() core.Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea ASOF '-5s' UNTIL '-1s'"},
		}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, fieldA, fieldB},
		}
		uf := core.Unflatten(g.Fields...)
		f := core.Flatten()
		uf.Connect(t)
		g.Connect(uf)
		f.Connect(g)
		return f
	})

	scenario("Group by partition key, pushdown allowed", "SELECT * FROM TableA GROUP BY x", func() core.Source {
		t := &testTable{"tablea"}
		g := &core.Group{
			Fields: []core.Field{sql.PointsField, core.NewField("a", eA), core.NewField("b", eB)},
			By:     []core.GroupBy{core.NewGroupBy("x", goexpr.Param("x"))},
		}
		f := core.Flatten()
		g.Connect(t)
		f.Connect(g)
		return f
	}, func() core.Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from TableA group by x"},
		}
		return t
	})
	//
	// scenario("SELECT * FROM TableA GROUP BY period(2s)", func() core.Source {
	// 	t := &testTable{"tablea"}
	// 	g := &core.Group{
	// 		Fields:     []core.Field{sql.PointsField, core.NewField("a", eA), core.NewField("b", eB)},
	// 		Resolution: 2 * time.Second,
	// 	}
	// 	f := core.Flatten()
	// 	g.Connect(t)
	// 	f.Connect(g)
	// 	return f
	// })
	//
	// scenario("SELECT *, a + b AS total FROM TableA ASOF '-5s' UNTIL '-1s' WHERE x > 5 GROUP BY y, period(2s) ORDER BY total DESC LIMIT 2, 5", func() core.Source {
	// 	t := &testTable{"tablea"}
	// 	fi := &core.RowFilter{
	// 		Label: "where x > 5",
	// 	}
	// 	g := &core.Group{
	// 		By:         []core.GroupBy{core.NewGroupBy("y", goexpr.Param("y"))},
	// 		Fields:     core.Fields{sql.PointsField, core.NewField("a", eA), core.NewField("b", eB), core.NewField("total", ADD(eA, eB))},
	// 		AsOf:       epoch.Add(-5 * time.Second),
	// 		Until:      epoch.Add(-1 * time.Second),
	// 		Resolution: 2 * time.Second,
	// 	}
	// 	f := core.Flatten()
	// 	s := core.Sort(core.NewOrderBy("total", true))
	// 	o := core.Offset(2)
	// 	l := core.Limit(5)
	// 	fi.Connect(t)
	// 	g.Connect(fi)
	// 	f.Connect(g)
	// 	s.Connect(f)
	// 	o.Connect(s)
	// 	l.Connect(o)
	// 	return l
	// })
	//
	// scenario("SELECT * FROM (SELECT * FROM TableA)", func() core.Source {
	// 	t := &testTable{"tablea"}
	// 	f := core.Flatten()
	// 	u := core.Unflatten()
	// 	f2 := core.Flatten()
	// 	f.Connect(t)
	// 	u.Connect(f)
	// 	f2.Connect(u)
	// 	return f2
	// })
	//
	// scenario("SELECT AVG(a) + AVG(b) AS total, * FROM (SELECT * FROM TableA)", func() core.Source {
	// 	avgTotal := core.NewField("total", ADD(AVG("a"), AVG("b")))
	// 	t := &testTable{"tablea"}
	// 	f := core.Flatten()
	// 	u := core.Unflatten(avgTotal, sql.PointsField, fieldA, fieldB)
	// 	g := &core.Group{
	// 		Fields: core.Fields{avgTotal, sql.PointsField, fieldA, fieldB},
	// 	}
	// 	f2 := core.Flatten()
	// 	f.Connect(t)
	// 	u.Connect(f)
	// 	g.Connect(u)
	// 	f2.Connect(g)
	// 	return f2
	// })

	for i, sqlStrings := range queries {
		opts := &Opts{
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
		}
		plan, err := Plan(opts, sqlStrings...)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, core.FormatSource(expected[i]()), core.FormatSource(plan), fmt.Sprintf("Non-clustered: %v: %v", descriptions[i], strings.Join(sqlStrings, " | ")))

		opts.QueryCluster = func(ctx context.Context, sqlString string, subQueryResults [][]interface{}, onRow core.OnFlatRow) error {
			return nil
		}
		opts.PartitionKeys = []string{"x", "y"}
		clusterPlan, err := Plan(opts, sqlStrings...)
		if len(sqlStrings) > 1 {
			assert.Equal(t, ErrNoMultipleOnCluster, err)
		} else if assert.NoError(t, err) {
			assert.Equal(t, core.FormatSource(expectedCluster[i]()), core.FormatSource(clusterPlan), fmt.Sprintf("Clustered: %v: %v", descriptions[i], strings.Join(sqlStrings, " | ")))
		}
	}
}

func TestPlanExecution(t *testing.T) {
	sqlString := `
SELECT *, AVG(a)+AVG(b) AS avg_total
FROM (SELECT * FROM tablea WHERE x IN (SELECT x FROM tablea WHERE x = 1) OR y IN (SELECT y FROM tablea WHERE y = 3))
HAVING avg_total > 20
ORDER BY _time
LIMIT 1
`

	opts := &Opts{
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
	}
	plan, err := Plan(opts, sqlString)
	if !assert.NoError(t, err) {
		return
	}

	var rows []*core.FlatRow
	plan.Iterate(core.Context(), func(row *core.FlatRow) (bool, error) {
		rows = append(rows, row)
		return true, nil
	})

	if assert.Len(t, rows, 1) {
		row := rows[0]
		assert.Equal(t, 1, row.Key.Get("x"))
		assert.Equal(t, 3, row.Key.Get("y"))
		assert.EqualValues(t, []float64{0, 50, 0, 50}, row.Values)
	}
}

type testTable struct {
	name string
}

func (t *testTable) GetFields() core.Fields {
	return core.Fields{sql.PointsField, core.NewField("a", eA), core.NewField("b", eB)}
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
	onRow(makeRow(epoch.Add(-9*resolution), 1, 0, 10, 0))
	onRow(makeRow(epoch.Add(-8*resolution), 0, 3, 0, 20))

	// Intentional gap
	onRow(makeRow(epoch.Add(-5*resolution), 1, 3, 50, 0))
	onRow(makeRow(epoch.Add(-4*resolution), 2, 5, 0, 60))
	onRow(makeRow(epoch.Add(-3*resolution), 1, 0, 70, 0))
	onRow(makeRow(epoch.Add(-2*resolution), 0, 3, 0, 80))
	onRow(makeRow(epoch.Add(-1*resolution), 1, 5, 90, 0))
	onRow(makeRow(epoch, 2, 2, 0, 100))
	return nil
}

// type partition emulates a partition in a cluster, partitioning by x then y
type partition struct {
	testTable
	partition     int
	numPartitions int
}

func (t *partition) Iterate(ctx context.Context, onRow core.OnRow) error {
	return t.testTable.Iterate(ctx, func(key bytemap.ByteMap, vals core.Vals) (bool, error) {
		x := key.Get("x")
		y := key.Get("y")
		if x != nil {
			if x.(int)%t.numPartitions == t.partition {
				onRow(key, vals)
			}
		}
		if y.(int)%t.numPartitions == t.partition {
			onRow(key, vals)
		}
		return true, nil
	})
}

func makeRow(ts time.Time, x int, y int, a float64, b float64) (bytemap.ByteMap, []encoding.Sequence) {
	keyMap := make(map[string]interface{}, 2)
	if x != 0 {
		keyMap["x"] = x
	}
	if y != 0 {
		keyMap["y"] = y
	}
	key := bytemap.New(keyMap)
	vals := make([]encoding.Sequence, 3)
	vals[0] = encoding.NewFloatValue(sql.PointsField.Expr, ts, 1)
	if a != 0 {
		vals[1] = encoding.NewFloatValue(eA, ts, a)
	}
	if b != 0 {
		vals[2] = encoding.NewFloatValue(eB, ts, b)
	}
	return key, vals
}

func (t *testTable) String() string {
	return t.name
}
