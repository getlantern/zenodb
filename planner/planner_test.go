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

	defaultFields = Fields{sql.PointsField, NewField("a", eA), NewField("b", eB)}

	groupByX = NewGroupBy("x", goexpr.Param("x"))
	groupByY = NewGroupBy("y", goexpr.Param("y"))
)

func noop(source RowSource) RowSource {
	return source
}

func flatten(source RowSource) Source {
	return Flatten(source)
}

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

	nonPushdownScenario := func(desc string, sqlString string, clusterSqlString string, prepare func(RowSource) RowSource, finish func(RowSource) Source, groupOpts GroupOpts) {
		scenario(desc, sqlString, func() Source {
			return finish(prepare(&testTable{"tablea", defaultFields}))
		}, func() Source {
			t := &clusterRowSource{
				clusterSource{
					query: &sql.Query{SQL: clusterSqlString},
				},
			}
			return finish(Group(t, groupOpts))
		})
	}

	pushdownScenario := func(desc string, sqlString string, clusterSqlString string, finish func(RowSource) Source) {
		scenario(desc, sqlString, func() Source {
			return finish(&testTable{"tablea", defaultFields})
		}, func() Source {
			return &clusterFlatRowSource{
				clusterSource{
					query: &sql.Query{SQL: clusterSqlString},
				},
			}
		})
	}

	nonPushdownScenario("No grouping",
		"SELECT * FROM TableA",
		"select * from TableA",
		noop,
		flatten,
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB},
		})

	nonPushdownScenario("WHERE clause",
		"SELECT * FROM TableA WHERE x = 'CN'",
		"select * from TableA where x = 'CN'",
		func(source RowSource) RowSource {
			return RowFilter(source, "where x = 'CN'", nil)
		},
		flatten,
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB},
		})

	nonPushdownScenario("WHERE with subquery",
		"SELECT * FROM TableA WHERE dim IN (SELECT DIM FROM tableb)",
		"select * from TableA where dim in (select dim from tableb)",
		func(source RowSource) RowSource {
			return RowFilter(source, "where dim in (select dim as dim from tableb)", nil)
		},
		flatten,
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB},
		})

	nonPushdownScenario("LIMIT and OFFSET",
		"SELECT * FROM TableA LIMIT 2, 5",
		"select * from TableA",
		noop,
		func(source RowSource) Source {
			return Limit(Offset(Flatten(source), 2), 5)
		},
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB},
		})

	fieldTotal := NewField("total", ADD(eA, eB))
	nonPushdownScenario("Calculated field",
		"SELECT *, a + b AS total FROM TableA",
		"select *, a+b as total from TableA",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields: Fields{sql.PointsField, fieldA, fieldB, fieldTotal},
			})
		},
		flatten,
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB, fieldTotal},
		})

	fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
	nonPushdownScenario("HAVING clause",
		"SELECT * FROM TableA HAVING a+b > 0",
		"select * from TableA",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields: Fields{sql.PointsField, fieldA, fieldB, fieldHaving},
			})
		},
		func(source RowSource) Source {
			return FlatRowFilter(Flatten(source), "a+b > 0", nil)
		},
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB, fieldHaving},
		})

	nonPushdownScenario("HAVING clause with single group by, pushdown not allowed",
		"SELECT * FROM TableA GROUP BY x HAVING a+b > 0",
		"select * from TableA group by x",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				By:     []GroupBy{groupByX},
				Fields: Fields{sql.PointsField, fieldA, fieldB, fieldHaving},
			})
		},
		func(source RowSource) Source {
			return FlatRowFilter(Flatten(source), "a+b > 0", nil)
		},
		GroupOpts{
			By:     []GroupBy{groupByX},
			Fields: Fields{sql.PointsField, fieldA, fieldB, fieldHaving},
		})

	pushdownScenario("HAVING clause with complete group by, pushdown allowed",
		"SELECT * FROM TableA GROUP BY y, x HAVING a+b > 0",
		"select * from TableA group by y, x having a+b > 0",
		func(source RowSource) Source {
			return FlatRowFilter(
				Flatten(
					Group(source, GroupOpts{
						Fields: Fields{sql.PointsField, fieldA, fieldB, fieldHaving},
						By:     []GroupBy{groupByX, groupByY},
					})), "a+b > 0", nil)
		})

	avgTotal := NewField("total", ADD(AVG("a"), AVG("b")))
	pushdownScenario("HAVING clause with complete group by and subselect, pushdown allowed",
		"SELECT AVG(a) + AVG(b) AS total FROM (SELECT * FROM TableA GROUP BY y, x) HAVING a+b > 0",
		"select avg(a)+avg(b) as total from (select * from TableA group by y, x) having a+b > 0",
		func(source RowSource) Source {
			fields := Fields{sql.PointsField, fieldA, fieldB}
			return FlatRowFilter(
				Flatten(
					Group(
						Unflatten(
							Flatten(
								Group(source, GroupOpts{
									Fields: fields,
									By:     []GroupBy{groupByX, groupByY},
								}),
							),
							avgTotal),
						GroupOpts{
							Fields: Fields{avgTotal, fieldHaving},
						},
					),
				), "a+b > 0", nil)
		})

	// nonPushdownScenario("HAVING clause with contiguous group by in subselect but discontiguous group by in main select, pushdown of subquery allowed",
	// 	"SELECT AVG(a) + AVG(b) AS total FROM (SELECT * FROM TableA GROUP BY y, CONCAT(',', x, 'thing') AS xplus) GROUP BY y, xplus HAVING a+b > 0",
	// 	"select * from TableA group by y, concat(',', x, 'thing') as xplus",
	// 	func(source RowSource) RowSource {
	// 		return Group(source, GroupOpts{
	// 			Fields: Fields{sql.PointsField, fieldA, fieldB},
	// 			By:     []GroupBy{NewGroupBy("xplus", goexpr.Concat(goexpr.Constant(","), goexpr.Param("x"), goexpr.Constant("thing"))), groupByY},
	// 		})
	// 	},
	// 	func(source RowSource) Source {
	// 		return FlatRowFilter(
	// 			Flatten(
	// 				Group(
	// 					Unflatten(Flatten(source), avgTotal),
	// 					GroupOpts{
	// 						Fields: Fields{avgTotal, fieldHaving},
	// 						By:     []GroupBy{NewGroupBy("xplus", goexpr.Param("xplus")), groupByY},
	// 					},
	// 				),
	// 			), "a+b > 0", nil)
	// 	},
	// 	GroupOpts{
	// 		Fields: Fields{avgTotal, fieldHaving},
	// 		By:     []GroupBy{NewGroupBy("xplus", goexpr.Param("xplus")), groupByY},
	// 	})

	scenario("HAVING clause with contiguous group by in subselect but discontiguous group by in main select, pushdown not allowed",
		"SELECT AVG(a) + AVG(b) AS total FROM (SELECT * FROM TableA GROUP BY y, CONCAT(',', x, 'thing') AS xplus) GROUP BY y, xplus HAVING a+b > 0",
		func() Source {
			fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
			avgTotal := NewField("total", ADD(AVG("a"), AVG("b")))
			fields := Fields{sql.PointsField, fieldA, fieldB}
			return FlatRowFilter(
				Flatten(
					Group(
						Unflatten(
							Flatten(
								Group(&testTable{"tablea", defaultFields}, GroupOpts{
									Fields: fields,
									By:     []GroupBy{NewGroupBy("xplus", goexpr.Concat(goexpr.Constant(","), goexpr.Param("x"), goexpr.Constant("thing"))), groupByY},
								}),
							),
							avgTotal),
						GroupOpts{
							Fields: Fields{avgTotal, fieldHaving},
							By:     []GroupBy{NewGroupBy("xplus", goexpr.Param("xplus")), groupByY},
						},
					),
				), "a+b > 0", nil)
		},
		func() Source {
			fieldHaving := NewField("_having", GT(ADD(eA, eB), CONST(0)))
			avgTotal := NewField("total", ADD(AVG("a"), AVG("b")))
			fields := Fields{avgTotal}
			t := &clusterFlatRowSource{
				clusterSource{
					query: &sql.Query{SQL: "select * from TableA group by y, concat(',', x, 'thing') as xplus"},
				},
			}
			return FlatRowFilter(
				Flatten(
					Group(
						Unflatten(t, fields...),
						GroupOpts{
							Fields: Fields{avgTotal, fieldHaving},
							By:     []GroupBy{NewGroupBy("xplus", goexpr.Param("xplus")), groupByY},
						},
					),
				), "a+b > 0", nil)
		})

	nonPushdownScenario("HAVING clause with discontiguous group by, pushdown not allowed",
		"SELECT * FROM TableA GROUP BY y HAVING a+b > 0",
		"select * from TableA group by y",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields: Fields{sql.PointsField, fieldA, fieldB, fieldHaving},
				By:     []GroupBy{groupByY},
			})
		},
		func(source RowSource) Source {
			return FlatRowFilter(Flatten(source), "a+b > 0", nil)
		},
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB, fieldHaving},
			By:     []GroupBy{groupByY},
		})

	nonPushdownScenario("HAVING clause with group by on non partition key, pushdown not allowed",
		"SELECT * FROM TableA GROUP BY CONCAT(',', z, 'thing') as zplus HAVING a+b > 0",
		"select * from TableA group by concat(',', z, 'thing') as zplus",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields: Fields{sql.PointsField, fieldA, fieldB, fieldHaving},
				By:     []GroupBy{NewGroupBy("zplus", goexpr.Concat(goexpr.Constant(","), goexpr.Param("z"), goexpr.Constant("thing")))},
			})
		},
		func(source RowSource) Source {
			return FlatRowFilter(Flatten(source), "a+b > 0", nil)
		},
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB, fieldHaving},
			By:     []GroupBy{NewGroupBy("zplus", goexpr.Param("zplus"))},
		})

	nonPushdownScenario("ASOF",
		"SELECT * FROM TableA ASOF '-5s'",
		"select * from TableA ASOF '-5s'",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields: Fields{sql.PointsField, fieldA, fieldB},
				AsOf:   epoch.Add(-5 * time.Second),
			})
		},
		flatten,
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB},
		})

	nonPushdownScenario("ASOF UNTIL",
		"SELECT * FROM TableA ASOF '-5s' UNTIL '-1s'",
		"select * from TableA ASOF '-5s' UNTIL '-1s'",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields: Fields{sql.PointsField, fieldA, fieldB},
				AsOf:   epoch.Add(-5 * time.Second),
				Until:  epoch.Add(-1 * time.Second),
			})
		},
		flatten,
		GroupOpts{
			Fields: Fields{sql.PointsField, fieldA, fieldB},
		})

	nonPushdownScenario("Change Resolution",
		"SELECT * FROM TableA GROUP BY period(2s)",
		"select * from TableA group by period(2 as s)",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields:     Fields{sql.PointsField, fieldA, fieldB},
				Resolution: 2 * time.Second,
			})
		},
		flatten,
		GroupOpts{
			Fields:     Fields{sql.PointsField, fieldA, fieldB},
			Resolution: 2 * time.Second,
		})

	nonPushdownScenario("Resolution smaller than data window",
		"SELECT * FROM TableA ASOF '-5s' UNTIL '-4s' GROUP BY period(2s)",
		"select * from TableA ASOF '-5s' UNTIL '-4s' group by period(2 as s)",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields:     Fields{sql.PointsField, fieldA, fieldB},
				AsOf:       epoch.Add(-5 * time.Second),
				Until:      epoch.Add(-4 * time.Second),
				Resolution: 1 * time.Second,
			})
		},
		flatten,
		GroupOpts{
			Fields:     Fields{sql.PointsField, fieldA, fieldB},
			Resolution: 1 * time.Second,
		})

	scenario("Complex SELECT", "SELECT *, a + b AS total FROM TableA ASOF '-5s' UNTIL '-1s' WHERE x = 'CN' GROUP BY y, period(2s) ORDER BY total DESC LIMIT 2, 5", func() Source {
		return Limit(
			Offset(
				Sort(
					Flatten(
						Group(
							RowFilter(&testTable{"tablea", defaultFields}, "where x = 'CN'", nil),
							GroupOpts{
								By:         []GroupBy{groupByY},
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
		t := &clusterRowSource{
			clusterSource{
				query: &sql.Query{SQL: "select *, a+b as total from TableA ASOF '-5s' UNTIL '-1s' where x = 'CN' group by y, period(2 as s)"},
			},
		}
		fields := Fields{sql.PointsField, fieldA, fieldB, NewField("total", ADD(eA, eB))}
		return Limit(Offset(Sort(Flatten(Group(t, GroupOpts{
			Fields:     fields,
			By:         []GroupBy{groupByY},
			Resolution: 2 * time.Second,
		})), NewOrderBy("total", true)), 2), 5)
	})

	for i, sqlString := range queries {
		opts := defaultOpts()
		plan, err := Plan(sqlString, opts)
		if assert.NoError(t, err) {
			assert.Equal(t, FormatSource(expected[i]()), FormatSource(plan), fmt.Sprintf("Non-clustered: %v: %v", descriptions[i], sqlString))
			verifyNoHaving(t, plan, sqlString)
		}

		opts.QueryCluster = queryCluster
		opts.PartitionBy = []string{"x", "y"}
		clusterPlan, err := Plan(sqlString, opts)
		if assert.NoError(t, err) {
			assert.Equal(t, FormatSource(expectedCluster[i]()), FormatSource(clusterPlan), fmt.Sprintf("Clustered: %v: %v", descriptions[i], sqlString))
			verifyNoHaving(t, plan, sqlString)
		}
	}
}

func verifyNoHaving(t *testing.T, plan Source, sqlString string) {
	for _, field := range plan.GetFields().Names() {
		if !assert.NotEqual(t, "_having", field, sqlString) {
			log.Debug(FormatSource(plan))
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

	verify := func(plan FlatRowSource) {
		var rows []*FlatRow
		plan.Iterate(context.Background(), func(row *FlatRow) (bool, error) {
			rows = append(rows, row)
			return true, nil
		})

		if assert.Len(t, rows, 1) {
			row := rows[0]
			assert.Equal(t, 1, row.Key.Get("x"))
			assert.Equal(t, 3, row.Key.Get("y"))
			assert.EqualValues(t, []float64{1, 50, 0, 50}, row.Values)
		}
	}

	opts := defaultOpts()
	plan, err := Plan(sqlString, opts)
	if !assert.NoError(t, err) {
		return
	}
	verify(plan)

	opts.QueryCluster = queryCluster
	plan, err = Plan(sqlString, opts)
	if !assert.NoError(t, err) {
		return
	}
	verify(plan)
}

func defaultOpts() *Opts {
	return &Opts{
		GetTable: func(table string, includedFields func(tableFields Fields) Fields) RowSource {
			return &testTable{table, includedFields(defaultFields)}
		},
		Now: func(table string) time.Time {
			return epoch
		},
		FieldSource: func(table string) (Fields, error) {
			return defaultFields, nil
		},
	}
}

func queryCluster(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onRow OnRow, onFlatRow OnFlatRow) error {
	parts := partitions(1)
	for _, part := range parts {
		opts := defaultOpts()
		opts.IsSubQuery = isSubQuery
		opts.SubQueryResults = subQueryResults
		opts.GetTable = func(table string, includedFields func(tableFields Fields) Fields) RowSource {
			part.name = table
			part.fields = includedFields(defaultFields)
			return part
		}
		plan, err := Plan(sqlString, opts)
		if err != nil {
			return err
		}
		if unflat {
			return UnflattenOptimized(plan).Iterate(ctx, onRow)
		} else {
			err = plan.Iterate(ctx, onFlatRow)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func partitions(num int) []*partition {
	partitions := make([]*partition, 0, num)
	for i := 0; i < num; i++ {
		partitions = append(partitions, &partition{partition: i, numPartitions: num})
	}
	return partitions
}

type testTable struct {
	name   string
	fields Fields
}

func (t *testTable) GetFields() Fields {
	return t.fields
}

func (t *testTable) GetGroupBy() []GroupBy {
	return []GroupBy{}
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
		} else if y.(int)%t.numPartitions == t.partition {
			onRow(key, vals)
		}
		return true, nil
	})
}

func (t *partition) String() string {
	return fmt.Sprintf("partition %v %d/%d", t.name, t.partition, t.numPartitions)
}
