package zenodb

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	. "github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/planner"

	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRoundTimeUp(t *testing.T) {
	ts := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	rounded := encoding.RoundTimeUp(ts, time.Second)
	expected := time.Date(2015, 5, 6, 7, 8, 10, 0, time.UTC)
	assert.Equal(t, expected, rounded)
}

func TestSingleDB(t *testing.T) {
	doTest(t, false, nil, func(tmpDir string, tmpFile string) (*DB, func(time.Time), func(string, func(*table, bool))) {
		db, err := NewDB(&DBOpts{
			Dir:            filepath.Join(tmpDir, "leader"),
			SchemaFile:     tmpFile,
			VirtualTime:    true,
			MaxMemoryRatio: 0.00001,
		})
		if !assert.NoError(t, err, "Unable to create leader DB") {
			t.Fatal()
		}
		return db, func(t time.Time) {
				db.clock.Advance(t)
			}, func(tableName string, cb func(tbl *table, isFollower bool)) {
				cb(db.getTable(tableName), false)
			}
	})
}

func TestClusterPushdownSinglePartition(t *testing.T) {
	doTestCluster(t, 1, []string{"r", "u"})
}

func TestClusterPushdownMultiPartition(t *testing.T) {
	doTestCluster(t, 7, []string{"r", "u"})
}

func TestClusterNoPushdownSinglePartition(t *testing.T) {
	doTestCluster(t, 1, nil)
}

func TestClusterNoPushdownMultiPartition(t *testing.T) {
	doTestCluster(t, 7, nil)
}

func doTestCluster(t *testing.T, numPartitions int, partitionBy []string) {
	doTest(t, true, partitionBy, func(tmpDir string, tmpFile string) (*DB, func(time.Time), func(string, func(*table, bool))) {
		leader, err := NewDB(&DBOpts{
			Dir:            filepath.Join(tmpDir, "leader"),
			SchemaFile:     tmpFile,
			VirtualTime:    true,
			Passthrough:    true,
			NumPartitions:  numPartitions,
			MaxMemoryRatio: 0.00001,
		})
		if !assert.NoError(t, err, "Unable to create leader DB") {
			t.Fatal()
		}

		followers := make([]*DB, 0, numPartitions)
		for i := 0; i < numPartitions; i++ {
			part := i
			follower, followerErr := NewDB(&DBOpts{
				Dir:            filepath.Join(tmpDir, fmt.Sprintf("follower%d", i)),
				SchemaFile:     tmpFile,
				VirtualTime:    true,
				NumPartitions:  numPartitions,
				Partition:      part,
				MaxMemoryRatio: 0.00001,
				Follow: func(f func() *common.Follow, cb func(data []byte, newOffset wal.Offset) error) {
					leader.Follow(f(), cb)
				},
				RegisterRemoteQueryHandler: func(partition int, query planner.QueryClusterFN) {
					var register func()
					register = func() {
						leader.RegisterQueryHandler(partition, func(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onFields core.OnFields, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
							// Re-register when finished
							defer register()
							return query(ctx, sqlString, isSubQuery, subQueryResults, unflat, onFields, onRow, onFlatRow)
						})
					}

					register()
				},
			})
			if !assert.NoError(t, followerErr, "Unable to create follower DB") {
				t.Fatal()
			}

			followers = append(followers, follower)
		}

		return leader, func(t time.Time) {
				leader.clock.Advance(t)
				for _, follower := range followers {
					follower.clock.Advance(t)
				}
			}, func(tableName string, cb func(tbl *table, isFollower bool)) {
				cb(leader.getTable(tableName), false)
				for _, follower := range followers {
					cb(follower.getTable(tableName), true)
				}
			}
	})
}

func doTest(t *testing.T, isClustered bool, partitionKeys []string, buildDB func(tmpDir string, tmpFile string) (*DB, func(time.Time), func(string, func(*table, bool)))) {
	rand.Seed(0)
	epoch := time.Date(2015, time.January, 1, 2, 3, 4, 5, time.UTC)

	tmpDir, err := ioutil.TempDir("", "zenodbtest")
	if !assert.NoError(t, err, "Unable to create temp directory") {
		return
	}
	defer os.RemoveAll(tmpDir)

	tmpFile, err := ioutil.TempFile("", "zenodbschema")
	if !assert.NoError(t, err, "Unable to create temp file") {
		return
	}
	tmpFile.Close()

	resolution := time.Second

	partitionClause := ""
	if len(partitionKeys) > 0 {
		partitionClause = fmt.Sprintf("\n  partitionby: [%v]\n", strings.Join(partitionKeys, ","))
	}
	schemaA := fmt.Sprintf(`
Test_a:
  maxflushlatency: 1ms
  retentionperiod: 200s%s
  sql: >
    SELECT
      IF(md = 'glub', SUM(i)) AS i,
      ii,
      i * ii / COUNT(ii) AS iii,
      AVG(iv) AS iv,
      AVG(BOUNDED(iv, 0, 10)) as biv,
      IF(TRUE = TRUE, PERCENTILE(p * 1, 99, 0, 1000, -1)) as pp,
      PERCENTILE(p, 5, 0, 1000, 2) AS pp_5p,
      z
    FROM inbound
    WHERE r = 'A'
    GROUP BY r, u, b, period(1s)
`, partitionClause)
	log.Debug(schemaA)
	err = ioutil.WriteFile(tmpFile.Name(), []byte(schemaA), 0644)
	if !assert.NoError(t, err, "Unable to write schemaA") {
		return
	}

	db, advanceClock, modifyTable := buildDB(tmpDir, tmpFile.Name())

	// TODO: verify that we can actually select successfully from the view.
	schemaB := schemaA + `
view_a:
  view: true
  maxflushlatency: 1ms
  retentionperiod: 200ms
  sql: >
    SELECT *
    FROM teSt_a
    WHERE r = 'A'
    GROUP BY u, b`
	log.Debug("Writing schemaB")
	err = ioutil.WriteFile(tmpFile.Name(), []byte(schemaB), 0644)
	if !assert.NoError(t, err, "Unable to write schemaB") {
		return
	}

	viewCreated := false
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		if db.getTable("view_a") != nil {
			viewCreated = true
		}
	}
	if !assert.True(t, viewCreated, "View failed to create within 5 seconds") {
		return
	}

	now := epoch
	advance := func(d time.Duration) {
		time.Sleep(250 * time.Millisecond)
		now = now.Add(d)
		advanceClock(now)
		time.Sleep(250 * time.Millisecond)
		for _, table := range []string{"test_a", "view_a"} {
			log.Debug(db.PrintTableStats(table))
		}
	}

	nextField := 0
	// This shuffles around fields to make sure that we're reading them correctly
	// from the file stores.
	shuffleFields := func() {
		time.Sleep(100 * time.Millisecond)
		modifyTable("test_a", func(tab *table, isFollower bool) {
			fields := tab.getFields()
			if !isFollower || rand.Float64() < 0.5 {
				// On followers, only shuffle fields 50% of the time
				fields[0], fields[1], fields[2] = fields[2], fields[0], fields[1]
			}
			nextField++
			newFields := make(core.Fields, 0, len(fields)+1)
			newFields = append(newFields, fields...)
			newFields = append(newFields, core.NewField(fmt.Sprintf("newfield_%d", nextField), AVG(fmt.Sprintf("n_%d", nextField))))
			tab.applyFields(newFields)
		})
		time.Sleep(100 * time.Millisecond)
	}

	randBelowResolution := func() time.Duration {
		return time.Duration(rand.Intn(int(resolution)))
	}

	db.Insert("inbound",
		now.Add(randBelowResolution()),
		map[string]interface{}{
			"r":  "A",
			"u":  1,
			"b":  false,
			"md": "glub",
		},
		map[string]float64{
			"i":  1,
			"ii": 2,
			"iv": 10,
		})
	shuffleFields()

	// Add a bunch of data for percentile calculation
	for i := float64(1); i <= 100; i++ {
		db.Insert("inbound",
			now,
			map[string]interface{}{
				"r":  "A",
				"u":  1,
				"b":  false,
				"md": "glub",
			},
			map[string]float64{
				"p": i,
			})
	}

	// This should get excluded by the filter
	db.Insert("inbound",
		now.Add(randBelowResolution()),
		map[string]interface{}{
			"r":  "B",
			"u":  1,
			"b":  false,
			"md": "glub",
		},
		map[string]float64{
			"i":  1,
			"ii": 2,
			"iv": 10,
		})
	shuffleFields()

	db.Insert("inbound",
		now.Add(randBelowResolution()),
		map[string]interface{}{
			"r":  "A",
			"u":  1,
			"b":  false,
			"md": "glub",
		},
		map[string]float64{
			"i":  10,
			"ii": 20,
			"iv": 20,
		})
	shuffleFields()

	advance(resolution)

	db.Insert("inbound",
		now.Add(randBelowResolution()),
		map[string]interface{}{
			"r":  "A",
			"u":  1,
			"b":  false,
			"md": "glub",
		},
		map[string]float64{
			"i":  111,
			"ii": 222,
			"iv": 30,
		})
	shuffleFields()

	db.Insert("inbound",
		now.Add(randBelowResolution()),
		map[string]interface{}{
			"r":  "A",
			"u":  2,
			"b":  false,
			"md": "glub",
		},
		map[string]float64{
			"i":  31,
			"ii": 42,
			"z":  53,
		})
	shuffleFields()

	db.Insert("inbound",
		now.Add(randBelowResolution()),
		map[string]interface{}{
			"r":  "A",
			"u":  2,
			"b":  true,
			"md": "glub",
		},
		map[string]float64{
			"i":  30000,
			"ii": 40000,
		})
	shuffleFields()

	scalingFactor := 5
	asOf := now.Add(time.Duration(1-scalingFactor) * resolution)
	until := now.Add(resolution)

	advance(5 * resolution)
	db.Insert("inbound",
		now.Add(randBelowResolution()),
		map[string]interface{}{
			"r":  "A",
			"u":  2,
			"b":  false,
			"md": "glub",
		},
		map[string]float64{
			"i":  500,
			"ii": 600,
			"z":  700,
		})
	shuffleFields()

	// Give processing time to catch up
	time.Sleep(10 * time.Second)

	table := db.getTable("test_a")
	fields := table.getFields()
	table.iterate(context.Background(), fields, true, func(dims bytemap.ByteMap, vals []encoding.Sequence) (bool, error) {
		log.Debugf("Dims: %v")
		for i, val := range vals {
			field := fields[i]
			log.Debugf("Table Dump %v : %v", field.Name, val.String(field.Expr, table.Resolution))
		}
		return true, nil
	})

	for _, includeMemStore := range []bool{true, false} {
		testSimpleQuery(t, db, includeMemStore, epoch, resolution)
		testCrosstabWithHavingQuery(t, db, includeMemStore, epoch, resolution)
		testStrideQuery(t, db, includeMemStore, epoch, resolution)
		testShiftQuery(t, db, includeMemStore, epoch, resolution)
		testSubQuery(t, db, includeMemStore, epoch, resolution)
		if false {
			testAggregateQuery(t, db, includeMemStore, now, epoch, resolution, asOf, until, scalingFactor)
		}
	}
}

func testSimpleQuery(t *testing.T, db *DB, includeMemStore bool, epoch time.Time, resolution time.Duration) {
	sqlString := `
SELECT *
FROM test_a
GROUP BY _
ORDER BY _time`

	epoch = encoding.RoundTimeUp(epoch, resolution)
	expectedResult{
		expectedRow{
			epoch,
			map[string]interface{}{},
			map[string]float64{
				"_points":    102,
				"i":          11,
				"ii":         22,
				"iii":        121,
				"iv":         15,
				"biv":        10,
				"pp":         90,
				"pp_5p":      5,
				"z":          0,
				"newfield_1": 0,
				"newfield_2": 0,
				"newfield_3": 0,
				"newfield_4": 0,
				"newfield_5": 0,
				"newfield_6": 0,
				"newfield_7": 0,
			},
		},
		expectedRow{
			epoch.Add(resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points":    3,
				"i":          30142,
				"ii":         40264,
				"iii":        404545829.3333333,
				"iv":         30,
				"biv":        0,
				"pp":         0,
				"pp_5p":      0,
				"z":          53,
				"newfield_1": 0,
				"newfield_2": 0,
				"newfield_3": 0,
				"newfield_4": 0,
				"newfield_5": 0,
				"newfield_6": 0,
				"newfield_7": 0,
			},
		},
		expectedRow{
			epoch.Add(6 * resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points":    1,
				"i":          500,
				"ii":         600,
				"iii":        300000,
				"iv":         0,
				"biv":        0,
				"pp":         0,
				"pp_5p":      0,
				"z":          700,
				"newfield_1": 0,
				"newfield_2": 0,
				"newfield_3": 0,
				"newfield_4": 0,
				"newfield_5": 0,
				"newfield_6": 0,
				"newfield_7": 0,
			},
		},
	}.assert(t, db, sqlString, includeMemStore)
}

// testHavingQuery makes sure that a HAVING clause works even when the field in
// that clause does not appear in the SELECT clause
func testCrosstabWithHavingQuery(t *testing.T, db *DB, includeMemStore bool, epoch time.Time, resolution time.Duration) {
	sqlString := `
SELECT i
FROM test_a
GROUP BY CROSSTAB(r)
HAVING biv = 10 AND i = 11
ORDER BY _time`

	epoch = encoding.RoundTimeUp(epoch, resolution)
	expectedResult{
		expectedRow{
			epoch,
			map[string]interface{}{},
			map[string]float64{
				"a_i": 11,
			},
		},
	}.assert(t, db, sqlString, includeMemStore)
}

func testStrideQuery(t *testing.T, db *DB, includeMemStore bool, epoch time.Time, resolution time.Duration) {
	sqlString := fmt.Sprintf(`
SELECT _points, i, ii, iii, iv, biv, z
FROM test_a
GROUP BY _, STRIDE(%v)
ORDER BY _time`, resolution*6)

	epoch = encoding.RoundTimeUp(epoch, resolution)
	expectedResult{
		expectedRow{
			epoch,
			map[string]interface{}{},
			map[string]float64{
				"_points": 102,
				"i":       11,
				"ii":      22,
				"iii":     121,
				"iv":      15,
				"biv":     10,
				"z":       0,
			},
		},
		expectedRow{
			epoch.Add(6 * resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points": 1,
				"i":       500,
				"ii":      600,
				"iii":     300000,
				"iv":      0,
				"biv":     0,
				"z":       700,
			},
		},
	}.assert(t, db, sqlString, includeMemStore)
}

func testShiftQuery(t *testing.T, db *DB, includeMemStore bool, epoch time.Time, resolution time.Duration) {
	sqlString := fmt.Sprintf(`
SELECT _points, CROSSHIFT(i, '%v', '%v') AS i
FROM test_a
GROUP BY _
HAVING i_1s > 0 OR i > 0
ORDER BY _time`, -2*resolution, 1*resolution)

	epoch = encoding.RoundTimeUp(epoch, resolution)
	expectedResult{
		// Excluded by HAVING filter
		expectedRow{
			epoch,
			map[string]interface{}{},
			map[string]float64{
				"_points": 102,
				"i":       11,
				"i_1s":    0,
			},
		},
		expectedRow{
			epoch.Add(resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points": 3,
				"i":       30142,
				"i_1s":    11,
			},
		},
		expectedRow{
			epoch.Add(2 * resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points": 0,
				"i":       0,
				"i_1s":    30142,
			},
		},
		expectedRow{
			epoch.Add(6 * resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points": 1,
				"i":       500,
				"i_1s":    0,
			},
		},
	}.assert(t, db, sqlString, includeMemStore)
}

func testSubQuery(t *testing.T, db *DB, includeMemStore bool, epoch time.Time, resolution time.Duration) {
	sqlString := `
SELECT _points, i
FROM (SELECT *
	FROM test_a
	GROUP BY _
	ORDER BY _time)`

	epoch = encoding.RoundTimeUp(epoch, resolution)
	expectedResult{
		expectedRow{
			epoch,
			map[string]interface{}{},
			map[string]float64{
				"_points": 102,
				"i":       11,
			},
		},
		expectedRow{
			epoch.Add(resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points": 3,
				"i":       30142,
			},
		},
		expectedRow{
			epoch.Add(6 * resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points": 1,
				"i":       500,
			},
		},
	}.assert(t, db, sqlString, includeMemStore)
}

func testAggregateQuery(t *testing.T, db *DB, includeMemStore bool, now time.Time, epoch time.Time, resolution time.Duration, asOf time.Time, until time.Time, scalingFactor int) {
	period := resolution * time.Duration(scalingFactor)

	sqlString := fmt.Sprintf(`
SELECT
	iii / 2 AS ciii,
	LOG2(iii) AS liii,
	IF(b != true, ii) AS ii,
	biv / 10 AS biv,
	*,
	IF(b = true, i) AS i_filtered,
	_points,
	LOG10(_points) AS lpoints,
	5.1 as cval,
	_ AS present
FROM test_a
ASOF '%s' UNTIL '%s'
WHERE b != true AND r IN (SELECT r FROM test_a)
GROUP BY r, u, period(%v)
HAVING ii * 2 = 488 OR ii = 42 OR unknown = 12
ORDER BY u DESC
`, asOf.In(time.UTC).Format(time.RFC3339), until.In(time.UTC).Format(time.RFC3339), period)

	expectedResult{
		expectedRow{
			encoding.RoundTimeDown(until, resolution),
			map[string]interface{}{
				"r": "A",
				"u": 2,
			},
			map[string]float64{
				"_points":    1,
				"lpoints":    math.Log10(1),
				"i_filtered": 0,
				"i":          31,
				"ii":         42,
				"iii":        float64(31*42) / float64(1),
				"iv":         0,
				"biv":        0,
				"ciii":       float64(31*42) / float64(1) / float64(2),
				"liii":       math.Log2(31 * 42),
				"cval":       5.1,
				"z":          53,
				"present":    1,
				"newfield_1": 0,
				"newfield_2": 0,
				"newfield_3": 0,
				"newfield_4": 0,
				"newfield_5": 0,
				"newfield_6": 0,
				"newfield_7": 0,
			},
		},
		expectedRow{
			encoding.RoundTimeDown(until, resolution),
			map[string]interface{}{
				"r": "A",
				"u": 1,
			},
			map[string]float64{
				"_points":    3,
				"lpoints":    math.Log10(3),
				"i_filtered": 0,
				"i":          122,
				"ii":         244,
				"iii":        float64(122*244) / float64(3),
				"iv":         20,
				"biv":        1,
				"ciii":       float64(122*244) / float64(3) / float64(2),
				"liii":       math.Log2(122 * 244),
				"cval":       5.1,
				"z":          0,
				"present":    1,
				"newfield_1": 0,
				"newfield_2": 0,
				"newfield_3": 0,
				"newfield_4": 0,
				"newfield_5": 0,
				"newfield_6": 0,
				"newfield_7": 0,
			},
		},
	}.assert(t, db, sqlString, includeMemStore)
}

type expectedResult []expectedRow

func (er expectedResult) assert(t *testing.T, db *DB, sqlString string, includeMemStore bool) {
	var rows []*core.FlatRow
	source, err := db.Query(sqlString, false, nil, includeMemStore)
	if !assert.NoError(t, err, "Unable to plan SQL query") {
		return
	}

	var fields core.Fields
	err = source.Iterate(context.Background(), func(inFields core.Fields) error {
		fields = inFields
		return nil
	}, func(row *core.FlatRow) (bool, error) {
		rows = append(rows, row)
		return true, nil
	})
	if !assert.NoError(t, err, "Unable to plan SQL query") {
		return
	}

	if false {
		spew.Dump(rows)
	}
	md := MetaDataFor(source, fields)
	if !assert.Len(t, rows, len(er), "Wrong number of rows, perhaps HAVING isn't working") {
		return
	}
	for i, erow := range er {
		row := rows[i]
		erow.assert(t, md.FieldNames, row, i+1)
	}
}

type expectedRow struct {
	ts   time.Time
	dims map[string]interface{}
	vals map[string]float64
}

func (erow expectedRow) assert(t *testing.T, fieldNames []string, row *core.FlatRow, idx int) {
	if !assert.Equal(t, erow.ts.In(time.UTC), encoding.TimeFromInt(row.TS).In(time.UTC), "Row %d - wrong timestamp", idx) {
		return
	}

	dims := row.Key.AsMap()
	if !assert.Len(t, dims, len(erow.dims), "Row %d - wrong number of dimensions in result", idx) {
		return
	}
	for k, v := range erow.dims {
		if !assert.Equal(t, v, dims[k], "Row %d - mismatch on dimension %v", idx, k) {
			return
		}
	}

	if !assert.Len(t, row.Values, len(erow.vals), "Row %d - wrong number of values in result", idx) {
		return
	}
	for i, v := range row.Values {
		fieldName := fieldNames[i]
		AssertFloatWithin(t, 0.01, erow.vals[fieldName], v, fmt.Sprintf("Row %d - mismatch on field %v", idx, fieldName))
	}
}
