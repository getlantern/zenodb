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
	"github.com/spaolacci/murmur3"
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

	pushdownScenario("No grouping",
		"SELECT * FROM TableA",
		"select * from TableA",
		func(source RowSource) Source {
			return Flatten(source)
		})

	pushdownScenario("Wildcard and specific grouping",
		"SELECT * FROM TableA GROUP BY *, a, b",
		"select * from TableA group by *, a, b",
		func(source RowSource) Source {
			return Flatten(source)
		})

	pushdownScenario("WHERE clause",
		"SELECT * FROM TableA WHERE x = 'CN'",
		"select * from TableA where x = 'CN'",
		func(source RowSource) Source {
			return Flatten(RowFilter(source, "where x = 'CN'", nil))
		})

	pushdownScenario("WHERE with subquery",
		"SELECT * FROM TableA WHERE dim IN (SELECT DIM FROM tableb)",
		"select * from TableA where dim in (select dim from tableb)",
		func(source RowSource) Source {
			return Flatten(RowFilter(source, "where dim in (select dim as dim from tableb)", nil))
		})

	scenario("LIMIT and OFFSET",
		"SELECT * FROM TableA LIMIT 2, 5",
		func() Source {
			return Limit(Offset(Flatten(&testTable{"tablea", defaultFields}), 2), 5)
		},
		func() Source {
			return Limit(Offset(
				&clusterFlatRowSource{
					clusterSource{
						query: &sql.Query{SQL: "select * from TableA limit 2, 5"},
					},
				}, 2), 5)
		})

	pushdownScenario("Calculated field",
		"SELECT *, a + b AS total FROM TableA",
		"select *, a+b as total from TableA",
		func(source RowSource) Source {
			return Flatten(Group(source, GroupOpts{
				Fields: textFieldSource("*, a+b as total"),
			}))
		})

	nonPushdownScenario("Unknown dim, pushdown not allowed",
		"SELECT * FROM TableA GROUP BY CONCAT('_', u, v) AS c",
		"select * from TableA group by u, v",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields: textFieldSource("*"),
				By:     []GroupBy{NewGroupBy("c", goexpr.Concat(goexpr.Constant("_"), goexpr.Param("u"), goexpr.Param("v")))},
			})
		},
		func(source RowSource) Source {
			return Flatten(source)
		},
		GroupOpts{
			Fields: textFieldSource("passthrough"),
			By:     []GroupBy{NewGroupBy("c", goexpr.Concat(goexpr.Constant("_"), goexpr.Param("u"), goexpr.Param("v")))},
		})

	nonPushdownScenario("CROSSTAB, pushdown not allowed",
		"SELECT * FROM TableA GROUP BY CROSSTAB(ct1, CONCAT('|', ct2))",
		"select * from TableA group by concat('_', ct1, concat('|', ct2)) as _crosstab",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields:   textFieldSource("*"),
				Crosstab: goexpr.Concat(goexpr.Constant("_"), goexpr.Param("ct1"), goexpr.Concat(goexpr.Constant("|"), goexpr.Param("ct2"))),
			})
		},
		func(source RowSource) Source {
			return Flatten(source)
		},
		GroupOpts{
			Fields:   textFieldSource("passthrough"),
			Crosstab: goexpr.Param("_crosstab"),
		})

	pushdownScenario("HAVING clause",
		"SELECT * FROM TableA HAVING a+b > 0",
		"select * from TableA having a+b > 0",
		func(source RowSource) Source {
			return FlatRowFilter(
				Flatten(
					Group(source, GroupOpts{
						Fields: textFieldSource("*"),
						Having: textExprSource("a+b > 0"),
					})), "a+b > 0", nil)
		})

	nonPushdownScenario("HAVING clause with single group by, pushdown not allowed",
		"SELECT * FROM TableA GROUP BY x HAVING a+b > 0",
		"select * from TableA group by x",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				By:     []GroupBy{groupByX},
				Fields: textFieldSource("*"),
				Having: textExprSource("a+b > 0"),
			})
		},
		func(source RowSource) Source {
			return FlatRowFilter(Flatten(source), "a+b > 0", nil)
		},
		GroupOpts{
			By:     []GroupBy{groupByX},
			Fields: textFieldSource("passthrough"),
			Having: textExprSource("a+b > 0"),
		})

	pushdownScenario("HAVING clause with complete group by, pushdown allowed",
		"SELECT * FROM TableA GROUP BY y, x HAVING a+b > 0",
		"select * from TableA group by y, x having a+b > 0",
		func(source RowSource) Source {
			return FlatRowFilter(
				Flatten(
					Group(source, GroupOpts{
						Fields: textFieldSource("*"),
						Having: textExprSource("a+b > 0"),
						By:     []GroupBy{groupByX, groupByY},
					})), "a+b > 0", nil)
		})

	nonPushdownScenario("HAVING clause with complete group by and CROSSTAB, pushdown not allowed",
		"SELECT * FROM TableA GROUP BY y, x, CROSSTAB(ct1, ct2) HAVING a+b > 0",
		"select * from TableA group by x, y, concat('_', ct1, ct2) as _crosstab",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				By:       []GroupBy{groupByX, groupByY},
				Crosstab: goexpr.Concat(goexpr.Constant("_"), goexpr.Param("ct1"), goexpr.Param("ct2")),
				Fields:   textFieldSource("*"),
				Having:   textExprSource("a+b > 0"),
			})
		},
		func(source RowSource) Source {
			return FlatRowFilter(Flatten(source), "a+b > 0", nil)
		},
		GroupOpts{
			By:       []GroupBy{groupByX, groupByY},
			Crosstab: goexpr.Param("_crosstab"),
			Fields:   textFieldSource("passthrough"),
			Having:   textExprSource("a+b > 0"),
		})

	pushdownScenario("HAVING clause with complete group by and subselect, pushdown allowed",
		"SELECT AVG(a) + AVG(b) AS total FROM (SELECT * FROM TableA GROUP BY y, x) HAVING a+b > 0",
		"select avg(a)+avg(b) as total from (select * from TableA group by y, x) having a+b > 0",
		func(source RowSource) Source {
			return FlatRowFilter(
				Flatten(
					Group(
						Unflatten(
							Flatten(
								Group(source, GroupOpts{
									Fields: textFieldSource("*"),
									By:     []GroupBy{groupByX, groupByY},
								}),
							),
							textFieldSource("avg(a)+avg(b) as total")),
						GroupOpts{
							Fields: textFieldSource("avg(a)+avg(b) as total"),
							Having: textExprSource("a+b > 0"),
						},
					),
				), "a+b > 0", nil)
		})

	groupByLenXInner := NewGroupBy("len_x", goexpr.Len(goexpr.Param("x")))
	groupByLenXOuter := NewGroupBy("len_x", goexpr.Param("len_x"))

	pushdownScenario("HAVING clause with complete group by and subselect using derived dimension, pushdown allowed",
		"SELECT AVG(a) + AVG(b) AS total FROM (SELECT * FROM TableA GROUP BY y, LEN(x) AS len_x) GROUP BY y, len_x HAVING a+b > 0",
		"select avg(a)+avg(b) as total from (select * from TableA group by y, len(x) as len_x) group by y, len_x having a+b > 0",
		func(source RowSource) Source {
			return FlatRowFilter(
				Flatten(
					Group(
						Unflatten(
							Flatten(
								Group(source, GroupOpts{
									Fields: textFieldSource("*"),
									By:     []GroupBy{groupByLenXInner, groupByY},
								}),
							),
							textFieldSource("avg(a)+avg(b) as total")),
						GroupOpts{
							Fields: textFieldSource("avg(a)+avg(b) as total"),
							Having: textExprSource("a+b > 0"),
							By:     []GroupBy{groupByLenXOuter, groupByY},
						},
					),
				), "a+b > 0", nil)
		})

	// nonPushdownScenario("HAVING clause with contiguous group by in subselect but discontiguous group by in main select, pushdown of subquery allowed",
	// 	"SELECT AVG(a) + AVG(b) AS total FROM (SELECT * FROM TableA GROUP BY y, CONCAT(',', x, 'thing') AS xplus) GROUP BY y, xplus HAVING a+b > 0",
	// 	"select * from TableA group by y, concat(',', x, 'thing') as xplus",
	// 	func(source RowSource) RowSource {
	// 		return Group(source, GroupOpts{
	// 			Fields: StaticFieldSource{sql.PointsField, fieldA, fieldB},
	// 			By:     []GroupBy{NewGroupBy("xplus", goexpr.Concat(goexpr.Constant(","), goexpr.Param("x"), goexpr.Constant("thing"))), groupByY},
	// 		})
	// 	},
	// 	func(source RowSource) Source {
	// 		return FlatRowFilter(
	// 			Flatten(
	// 				Group(
	// 					Unflatten(Flatten(source), avgTotal),
	// 					GroupOpts{
	// 						Fields: StaticFieldSource{avgTotal, fieldHaving},
	// 						By:     []GroupBy{NewGroupBy("xplus", goexpr.Param("xplus")), groupByY},
	// 					},
	// 				),
	// 			), "a+b > 0", nil)
	// 	},
	// 	GroupOpts{
	// 		Fields: StaticFieldSource{avgTotal, fieldHaving},
	// 		By:     []GroupBy{NewGroupBy("xplus", goexpr.Param("xplus")), groupByY},
	// 	})

	scenario("HAVING clause with complete group by in subselect but incomplete group by in main select, pushdown not allowed",
		"SELECT AVG(a) + AVG(b) AS total FROM (SELECT * FROM TableA GROUP BY y, LEN(x) AS xplus) GROUP BY y HAVING a+b > 0",
		func() Source {
			return FlatRowFilter(
				Flatten(
					Group(
						Unflatten(
							Flatten(
								Group(&testTable{"tablea", defaultFields}, GroupOpts{
									Fields: textFieldSource("*"),
									By:     []GroupBy{NewGroupBy("xplus", goexpr.Len(goexpr.Param("x"))), groupByY},
								}),
							),
							textFieldSource("avg(a)+avg(b) as total")),
						GroupOpts{
							Fields: textFieldSource("avg(a)+avg(b) as total"),
							Having: textExprSource("a+b > 0"),
							By:     []GroupBy{groupByY},
						},
					),
				), "a+b > 0", nil)
		},
		func() Source {
			t := &clusterFlatRowSource{
				clusterSource{
					query: &sql.Query{SQL: "select * from TableA group by y, len(x) as xplus"},
				},
			}
			return FlatRowFilter(
				Flatten(
					Group(
						Unflatten(t, textFieldSource("avg(a)+avg(b) as total")),
						GroupOpts{
							Fields: textFieldSource("avg(a)+avg(b) as total"),
							Having: textExprSource("a+b > 0"),
							By:     []GroupBy{groupByY},
						},
					),
				), "a+b > 0", nil)
		})

	nonPushdownScenario("HAVING clause with discontiguous group by, pushdown not allowed",
		"SELECT * FROM TableA GROUP BY y HAVING a+b > 0",
		"select * from TableA group by y",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields: textFieldSource("*"),
				Having: textExprSource("a+b > 0"),
				By:     []GroupBy{groupByY},
			})
		},
		func(source RowSource) Source {
			return FlatRowFilter(Flatten(source), "a+b > 0", nil)
		},
		GroupOpts{
			Fields: textFieldSource("passthrough"),
			Having: textExprSource("a+b > 0"),
			By:     []GroupBy{groupByY},
		})

	nonPushdownScenario("HAVING clause with group by on non partition key, pushdown not allowed",
		"SELECT * FROM TableA GROUP BY CONCAT(',', z, 'thing') as zplus HAVING a+b > 0",
		"select * from TableA group by z",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields: textFieldSource("*"),
				Having: textExprSource("a+b > 0"),
				By:     []GroupBy{NewGroupBy("zplus", goexpr.Concat(goexpr.Constant(","), goexpr.Param("z"), goexpr.Constant("thing")))},
			})
		},
		func(source RowSource) Source {
			return FlatRowFilter(Flatten(source), "a+b > 0", nil)
		},
		GroupOpts{
			Fields: textFieldSource("passthrough"),
			Having: textExprSource("a+b > 0"),
			By:     []GroupBy{NewGroupBy("zplus", goexpr.Concat(goexpr.Constant(","), goexpr.Param("z"), goexpr.Constant("thing")))},
		})

	pushdownScenario("ASOF",
		"SELECT * FROM TableA ASOF '-5s'",
		"select * from TableA ASOF '-5s'",
		func(source RowSource) Source {
			return Flatten(Group(source, GroupOpts{
				Fields: textFieldSource("*"),
				AsOf:   epoch.Add(-5 * time.Second),
			}))
		})

	pushdownScenario("ASOF UNTIL",
		"SELECT * FROM TableA ASOF '-5s' UNTIL '-1s'",
		"select * from TableA ASOF '-5s' UNTIL '-1s'",
		func(source RowSource) Source {
			return Flatten(Group(source, GroupOpts{
				Fields: textFieldSource("*"),
				AsOf:   epoch.Add(-5 * time.Second),
				Until:  epoch.Add(-1 * time.Second),
			}))
		})

	nonPushdownScenario("Change Resolution",
		"SELECT * FROM TableA GROUP BY period(2s)",
		"select * from TableA group by period(2 as s)",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields:     textFieldSource("*"),
				Resolution: 2 * time.Second,
			})
		},
		flatten,
		GroupOpts{
			Fields: textFieldSource("passthrough"),
		})

	nonPushdownScenario("Resolution smaller than data window",
		"SELECT * FROM TableA ASOF '-5s' UNTIL '-4s' GROUP BY period(2s)",
		"select * from TableA ASOF '-5s' UNTIL '-4s' group by period(2 as s)",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields:     textFieldSource("*"),
				AsOf:       epoch.Add(-5 * time.Second),
				Until:      epoch.Add(-4 * time.Second),
				Resolution: 1 * time.Second,
			})
		},
		flatten,
		GroupOpts{
			Fields: textFieldSource("passthrough"),
		})

	nonPushdownScenario("Stride",
		"SELECT * FROM TableA GROUP BY stride(4s)",
		"select * from TableA group by stride(4 as s)",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields:      textFieldSource("*"),
				Resolution:  4 * time.Second,
				StrideSlice: resolution,
			})
		},
		flatten,
		GroupOpts{
			Fields: textFieldSource("passthrough"),
		})

	nonPushdownScenario("Stride with period",
		"SELECT * FROM TableA GROUP BY period(2s), stride(4s)",
		"select * from TableA group by period(2 as s), stride(4 as s)",
		func(source RowSource) RowSource {
			return Group(source, GroupOpts{
				Fields:      textFieldSource("*"),
				Resolution:  4 * time.Second,
				StrideSlice: 2 * time.Second,
			})
		},
		flatten,
		GroupOpts{
			Fields: textFieldSource("passthrough"),
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
								Fields:     textFieldSource("*, a+b as total"),
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
		return Limit(Offset(Sort(Flatten(Group(t, GroupOpts{
			Fields: textFieldSource("passthrough"),
			By:     []GroupBy{groupByY},
		})), NewOrderBy("total", true)), 2), 5)
	})

	for i, sqlString := range queries {
		opts := defaultOpts()
		plan, err := Plan(sqlString, opts)
		if assert.NoError(t, err, sqlString) {
			assert.Equal(t, FormatSource(expected[i]()), FormatSource(plan), fmt.Sprintf("Non-clustered: %v: %v", descriptions[i], sqlString))
		}

		opts.QueryCluster = queryCluster
		clusterPlan, err := Plan(sqlString, opts)
		if assert.NoError(t, err, sqlString) {
			assert.Equal(t, FormatSource(expectedCluster[i]()), FormatSource(clusterPlan), fmt.Sprintf("Clustered: %v: %v", descriptions[i], sqlString))
		}
	}
}

func verifyNoHaving(t *testing.T, fields Fields, plan Source, sqlString string) {
	for _, field := range fields.Names() {
		if !assert.NotEqual(t, "_having", field, sqlString) {
			log.Debug(FormatSource(plan))
		}
	}
}

func TestPlanExecution(t *testing.T) {
	sqlString := `
SELECT AVG(a)+AVG(b) AS avg_total
FROM (SELECT * FROM tablea WHERE x IN (SELECT x FROM tablea WHERE x = 1) OR y IN (SELECT y FROM tablea WHERE y = 3))
HAVING avg_total > 20
ORDER BY _time
LIMIT 1
`

	verify := func(plan FlatRowSource) {
		var rows []*FlatRow
		plan.Iterate(context.Background(), func(fields Fields) error {
			verifyNoHaving(t, fields, plan, sqlString)
			return nil
		}, func(row *FlatRow) (bool, error) {
			rows = append(rows, row)
			return true, nil
		})

		if assert.Len(t, rows, 1) {
			row := rows[0]
			assert.Equal(t, 1, row.Key.Get("x"))
			assert.Equal(t, 3, row.Key.Get("y"))
			assert.EqualValues(t, []float64{50}, row.Values)
		}
	}

	opts := defaultOpts()
	plan, err := Plan(sqlString, opts)
	if !assert.NoError(t, err) {
		return
	}
	log.Debug(FormatSource(plan))
	verify(plan)

	opts.QueryCluster = queryCluster
	plan, err = Plan(sqlString, opts)
	if !assert.NoError(t, err) {
		return
	}
	log.Debug(FormatSource(plan))
	verify(plan)
}

func defaultOpts() *Opts {
	return &Opts{
		GetTable: func(table string, includedFields func(tableFields Fields) (Fields, error)) (Table, error) {
			included, err := includedFields(defaultFields)
			if err != nil {
				return nil, err
			}
			return &testTable{table, included}, nil
		},
		Now: func(table string) time.Time {
			return epoch
		},
	}
}

func queryCluster(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onFields OnFields, onRow OnRow, onFlatRow OnFlatRow) error {
	numPartitions := 1
	for i := 0; i < numPartitions; i++ {
		opts := defaultOpts()
		opts.IsSubQuery = isSubQuery
		opts.SubQueryResults = subQueryResults
		opts.GetTable = func(table string, includedFields func(tableFields Fields) (Fields, error)) (Table, error) {
			part := &partition{
				partition:     i,
				numPartitions: numPartitions,
			}
			part.name = table
			var err error
			part.fields, err = includedFields(defaultFields)
			return part, err
		}
		plan, err := Plan(sqlString, opts)
		if err != nil {
			return err
		}
		if unflat {
			return UnflattenOptimized(plan).Iterate(ctx, onFields, onRow)
		} else {
			err = plan.Iterate(ctx, onFields, onFlatRow)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type testTable struct {
	name   string
	fields Fields
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

func (t *testTable) GetPartitionBy() []string {
	return []string{"x", "y"}
}

func (t *testTable) Iterate(ctx context.Context, onFields OnFields, onRow OnRow) error {
	onFields(t.fields)

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

func (t *partition) Iterate(ctx context.Context, onFields OnFields, onRow OnRow) error {
	return t.testTable.Iterate(ctx, onFields, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		// This mimics the logic in the actual clustering code
		h := murmur3.New32()
		x := key.GetBytes("x")
		y := key.GetBytes("y")
		if len(x) > 0 {
			h.Write(x)
		}
		if len(y) > 0 {
			h.Write(y)
		}
		if int(h.Sum32())%t.numPartitions == t.partition {
			onRow(key, vals)
		}
		return true, nil
	})
}

func (t *partition) String() string {
	return fmt.Sprintf("partition %v %d/%d", t.name, t.partition, t.numPartitions)
}

type textFieldSource string

func (tfs textFieldSource) Get(known Fields) (Fields, error) {
	return nil, nil
}

func (tfs textFieldSource) String() string {
	return string(tfs)
}

type textExprSource string

func (tes textExprSource) Get(known Fields) (Expr, error) {
	return nil, nil
}

func (tes textExprSource) String() string {
	return string(tes)
}
