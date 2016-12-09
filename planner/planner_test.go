package planner

import (
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
	. "github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	. "github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/sql"
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

	fieldA = NewField("a", eA)
	fieldB = NewField("b", eB)
)

func TestPlans(t *testing.T) {
	var descriptions []string
	var queries []string
	var expected []func() Source
	var expectedCluster []func() Source

	scenario := func(desc string, sqlString string, sourceFn func() Source, clusterSourceFn func() Source) {
		descriptions = append(descriptions, desc)
		queries = append(queries, sqlString)
		expected = append(expected, sourceFn)
		expectedCluster = append(expectedCluster, clusterSourceFn)
	}

	scenario("No grouping", "SELECT * FROM TableA", func() Source {
		return Flatten(&testTable{"tablea"})
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea"},
		}
		return Flatten(Group(Unflatten(t), GroupOpts{
			Fields: []Field{sql.PointsField, fieldA, fieldB},
		}))
	})

	scenario("WHERE clause", "SELECT * FROM TableA WHERE x > 5", func() Source {
		return Flatten(RowFilter(&testTable{"tablea"}, "where x > 5", nil))
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea where x > 5"},
		}
		fields := []Field{sql.PointsField, fieldA, fieldB}
		return Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields: fields,
		}))
	})

	scenario("WHERE with subquery", "SELECT * FROM TableA WHERE dim IN (SELECT DIM FROM tableb)", func() Source {
		return Flatten(RowFilter(&testTable{"tablea"}, "where dim in (select dim as dim from tableb)", nil))
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea where dim in (select dim from tableb)"},
		}
		fields := []Field{sql.PointsField, fieldA, fieldB}
		return Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields: fields,
		}))
	})

	scenario("LIMIT and OFFSET", "SELECT * FROM TableA LIMIT 2, 5", func() Source {
		return Limit(Offset(Flatten(&testTable{"tablea"}), 2), 5)
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea"},
		}
		fields := []Field{sql.PointsField, fieldA, fieldB}
		return Limit(Offset(Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields: fields,
		})), 2), 5)
	})

	scenario("Calculated field", "SELECT *, a + b AS total FROM TableA", func() Source {
		fieldTotal := NewField("total", ADD(eA, eB))
		return Flatten(Group(&testTable{"tablea"}, GroupOpts{
			Fields: []Field{sql.PointsField, fieldA, fieldB, fieldTotal},
		}))
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select *, a+b as total from tablea"},
		}
		fieldTotal := NewField("total", ADD(eA, eB))
		fields := []Field{sql.PointsField, fieldA, fieldB, fieldTotal}
		return Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields: fields,
		}))
	})

	scenario("HAVING clause", "SELECT * FROM TableA HAVING a+b > 0", func() Source {
		fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
		return FlatRowFilter(Flatten(Group(&testTable{"tablea"}, GroupOpts{
			Fields: []Field{sql.PointsField, fieldA, fieldB, fieldHaving},
		})), "a+b > 0", nil)
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea"},
		}
		fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
		fields := []Field{sql.PointsField, fieldA, fieldB, fieldHaving}
		return FlatRowFilter(Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields: fields,
		})), "a+b > 0", nil)
	})

	scenario("HAVING clause with single group by, pushdown allowed", "SELECT * FROM TableA GROUP BY x HAVING a+b > 0", func() Source {
		fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
		return FlatRowFilter(Flatten(Group(&testTable{"tablea"}, GroupOpts{
			Fields: []Field{sql.PointsField, fieldA, fieldB, fieldHaving},
			By:     []GroupBy{NewGroupBy("x", goexpr.Param("x"))},
		})), "a+b > 0", nil)
	}, func() Source {
		return &clusterSource{
			query: &sql.Query{SQL: "select * from TableA group by x having a+b > 0"},
		}
	})

	scenario("HAVING clause with contiguous group by, pushdown allowed", "SELECT * FROM TableA GROUP BY y, x HAVING a+b > 0", func() Source {
		fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
		return FlatRowFilter(Flatten(Group(&testTable{"tablea"}, GroupOpts{
			Fields: []Field{sql.PointsField, fieldA, fieldB, fieldHaving},
			By:     []GroupBy{NewGroupBy("x", goexpr.Param("x")), NewGroupBy("y", goexpr.Param("y"))},
		})), "a+b > 0", nil)
	}, func() Source {
		return &clusterSource{
			query: &sql.Query{SQL: "select * from TableA group by y, x having a+b > 0"},
		}
	})

	scenario("HAVING clause with contiguous group by and subselect, pushdown allowed", "SELECT AVG(a) + AVG(b) AS total FROM (SELECT * FROM TableA GROUP BY y, x) HAVING a+b > 0", func() Source {
		fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
		avgTotal := NewField("total", ADD(AVG("a"), AVG("b")))
		fields := []Field{sql.PointsField, fieldA, fieldB}
		return FlatRowFilter(
			Flatten(
				Group(
					Unflatten(
						Flatten(
							Group(&testTable{"tablea"}, GroupOpts{
								Fields: fields,
								By:     []GroupBy{NewGroupBy("x", goexpr.Param("x")), NewGroupBy("y", goexpr.Param("y"))},
							}),
						),
						fields...),
					GroupOpts{
						Fields: []Field{avgTotal, fieldHaving},
					},
				),
			), "a+b > 0", nil)
	}, func() Source {
		return &clusterSource{
			query: &sql.Query{SQL: "select avg(a)+avg(b) as total from (select * from TableA group by y, x) having a+b > 0"},
		}
	})

	scenario("HAVING clause with discontiguous group by, pushdown not allowed", "SELECT * FROM TableA GROUP BY y HAVING a+b > 0", func() Source {
		fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
		return FlatRowFilter(Flatten(Group(&testTable{"tablea"}, GroupOpts{
			Fields: []Field{sql.PointsField, fieldA, fieldB, fieldHaving},
			By:     []GroupBy{NewGroupBy("y", goexpr.Param("y"))},
		})), "a+b > 0", nil)
	}, func() Source {
		fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea group by y"},
		}
		fields := []Field{sql.PointsField, fieldA, fieldB, fieldHaving}
		return FlatRowFilter(Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields: fields,
			By:     []GroupBy{NewGroupBy("y", goexpr.Param("y"))},
		})), "a+b > 0", nil)
	})

	scenario("HAVING clause with group by on non partition key, pushdown not allowed", "SELECT * FROM TableA GROUP BY z HAVING a+b > 0", func() Source {
		fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
		return FlatRowFilter(Flatten(Group(&testTable{"tablea"}, GroupOpts{
			Fields: []Field{sql.PointsField, fieldA, fieldB, fieldHaving},
			By:     []GroupBy{NewGroupBy("z", goexpr.Param("z"))},
		})), "a+b > 0", nil)
	}, func() Source {
		fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea group by z"},
		}
		fields := []Field{sql.PointsField, fieldA, fieldB, fieldHaving}
		return FlatRowFilter(Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields: fields,
			By:     []GroupBy{NewGroupBy("z", goexpr.Param("z"))},
		})), "a+b > 0", nil)
	})

	scenario("ASOF", "SELECT * FROM TableA ASOF '-5s'", func() Source {
		return Flatten(Group(&testTable{"tablea"}, GroupOpts{
			Fields: []Field{sql.PointsField, fieldA, fieldB},
			AsOf:   epoch.Add(-5 * time.Second),
		}))
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea ASOF '-5s'"},
		}
		fields := []Field{sql.PointsField, fieldA, fieldB}
		return Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields: fields,
		}))
	})

	scenario("ASOF UNTIL", "SELECT * FROM TableA ASOF '-5s' UNTIL '-1s'", func() Source {
		return Flatten(Group(&testTable{"tablea"}, GroupOpts{
			Fields: []Field{sql.PointsField, fieldA, fieldB},
			AsOf:   epoch.Add(-5 * time.Second),
			Until:  epoch.Add(-1 * time.Second),
		}))
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea ASOF '-5s' UNTIL '-1s'"},
		}
		fields := []Field{sql.PointsField, fieldA, fieldB}
		return Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields: fields,
		}))
	})

	scenario("Change Resolution", "SELECT * FROM TableA GROUP BY period(2s)", func() Source {
		return Flatten(Group(&testTable{"tablea"}, GroupOpts{
			Fields:     []Field{sql.PointsField, fieldA, fieldB},
			Resolution: 2 * time.Second,
		}))
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select * from tablea group by period(2 as s)"},
		}
		fields := []Field{sql.PointsField, fieldA, fieldB}
		return Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields:     fields,
			Resolution: 2 * time.Second,
		}))
	})
	//
	scenario("Complex SELECT", "SELECT *, a + b AS total FROM TableA ASOF '-5s' UNTIL '-1s' WHERE x > 5 GROUP BY y, period(2s) ORDER BY total DESC LIMIT 2, 5", func() Source {
		return Limit(
			Offset(
				Sort(
					Flatten(
						Group(
							RowFilter(&testTable{"tablea"}, "where x > 5", nil),
							GroupOpts{
								By:         []GroupBy{NewGroupBy("y", goexpr.Param("y"))},
								Fields:     Fields{sql.PointsField, NewField("a", eA), NewField("b", eB), NewField("total", ADD(eA, eB))},
								AsOf:       epoch.Add(-5 * time.Second),
								Until:      epoch.Add(-1 * time.Second),
								Resolution: 2 * time.Second,
							}),
					), NewOrderBy("total", true),
				), 2,
			), 5,
		)
	}, func() Source {
		t := &clusterSource{
			query: &sql.Query{SQL: "select *, a+b as total from tablea ASOF '-5s' UNTIL '-1s' where x > 5 group by y, period(2 as s)"},
		}
		fields := []Field{sql.PointsField, fieldA, fieldB, NewField("total", ADD(eA, eB))}
		return Limit(Offset(Sort(Flatten(Group(Unflatten(t, fields...), GroupOpts{
			Fields:     fields,
			By:         []GroupBy{NewGroupBy("y", goexpr.Param("y"))},
			Resolution: 2 * time.Second,
		})), NewOrderBy("total", true)), 2), 5)
	})

	for i, sqlString := range queries {
		opts := &Opts{
			GetTable: func(table string) RowSource {
				return &testTable{table}
			},
			Now: func(table string) time.Time {
				return epoch
			},
			FieldSource: func(table string) ([]Field, error) {
				t := &testTable{table}
				return t.GetFields(), nil
			},
		}
		plan, err := Plan(sqlString, opts)
		if assert.NoError(t, err) {
			assert.Equal(t, FormatSource(expected[i]()), FormatSource(plan), fmt.Sprintf("Non-clustered: %v: %v", descriptions[i], sqlString))
		}

		opts.QueryCluster = func(ctx context.Context, sqlString string, subQueryResults [][]interface{}, onRow OnFlatRow) error {
			return nil
		}
		opts.PartitionKeys = []string{"x", "y"}
		clusterPlan, err := Plan(sqlString, opts)
		if assert.NoError(t, err) {
			assert.Equal(t, FormatSource(expectedCluster[i]()), FormatSource(clusterPlan), fmt.Sprintf("Clustered: %v: %v", descriptions[i], sqlString))
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
		GetTable: func(table string) RowSource {
			return &testTable{table}
		},
		Now: func(table string) time.Time {
			return epoch
		},
		FieldSource: func(table string) ([]Field, error) {
			t := &testTable{table}
			return t.GetFields(), nil
		},
	}
	plan, err := Plan(sqlString, opts)
	if !assert.NoError(t, err) {
		return
	}

	var rows []*FlatRow
	plan.Iterate(Context(), func(row *FlatRow) (bool, error) {
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

func (t *testTable) GetFields() Fields {
	return Fields{sql.PointsField, NewField("a", eA), NewField("b", eB)}
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

func (t *testTable) Iterate(ctx context.Context, onRow OnRow) error {
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

func (t *partition) Iterate(ctx context.Context, onRow OnRow) error {
	return t.testTable.Iterate(ctx, func(key bytemap.ByteMap, vals Vals) (bool, error) {
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
