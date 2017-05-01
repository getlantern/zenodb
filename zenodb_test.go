package zenodb

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	doTest(t, false, nil, func(tmpDir string, tmpFile string) (*DB, func(time.Time), func(string, func(*table))) {
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
			}, func(tableName string, cb func(tbl *table)) {
				cb(db.getTable(tableName))
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
	doTest(t, true, partitionBy, func(tmpDir string, tmpFile string) (*DB, func(time.Time), func(string, func(*table))) {
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
			}, func(tableName string, cb func(tbl *table)) {
				cb(leader.getTable(tableName))
				for _, follower := range followers {
					cb(follower.getTable(tableName))
				}
			}
	})
}

func doTest(t *testing.T, isClustered bool, partitionKeys []string, buildDB func(tmpDir string, tmpFile string) (*DB, func(time.Time), func(string, func(*table)))) {
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

	// This shuffles around fields to make sure that we're reading them correctly
	// from the file stores.
	shuffleFields := func() {
		time.Sleep(100 * time.Millisecond)
		if true {
			// TODO: make sure that schema evolution works.
			return
		}
		modifyTable("test_a", func(tab *table) {
			tab.Fields[0], tab.Fields[1], tab.Fields[2] = tab.Fields[1], tab.Fields[2], tab.Fields[0]
		})
		time.Sleep(100 * time.Millisecond)
	}

	randAboveRes := func() time.Duration {
		return time.Duration(1 * rand.Intn(int(resolution)))
	}

	db.Insert("inbound",
		now.Add(randAboveRes()),
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

	// This should get excluded by the filter
	db.Insert("inbound",
		now.Add(randAboveRes()),
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
		now.Add(randAboveRes()),
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

	// Change the schema a bit
	if false {
		// TODO: make sure that schema evolution works.
		modifyTable("test_a", func(tab *table) {
			newFields := make([]core.Field, 0, len(tab.Fields)+1)
			newFields = append(newFields, core.NewField("newfield", AVG("h")))
			for _, field := range tab.Fields {
				newFields = append(newFields, field)
			}
			tab.Fields = newFields
		})
	}

	advance(resolution + randAboveRes())

	db.Insert("inbound",
		now,
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
		now,
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
		now,
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

	advance(5 * randAboveRes())
	db.Insert("inbound",
		now,
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
	table.iterate(table.Fields.Names(), true, func(dims bytemap.ByteMap, vals []encoding.Sequence) (bool, error) {
		log.Debugf("Dims: %v")
		for i, field := range table.Fields {
			log.Debugf("Table Dump %v : %v", field.Name, vals[i].String(field.Expr, table.Resolution))
		}
		return true, nil
	})

	for _, includeMemStore := range []bool{true, false} {
		testSimpleQuery(t, db, includeMemStore, epoch, resolution)
		testStrideQuery(t, db, includeMemStore, epoch, resolution)
		testSubQuery(t, db, includeMemStore, epoch, resolution)
		testAggregateQuery(t, db, includeMemStore, now, epoch, resolution, asOf, until, scalingFactor)
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
				"_points": 2,
				"i":       11,
				"ii":      22,
				"iii":     121,
				"iv":      15,
				"biv":     10,
				"z":       0,
			},
		},
		expectedRow{
			epoch.Add(resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points": 3,
				"i":       30142,
				"ii":      40264,
				"iii":     404545829.3333333,
				"iv":      30,
				"biv":     0,
				"z":       53,
			},
		},
		expectedRow{
			epoch.Add(4 * resolution),
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

func testStrideQuery(t *testing.T, db *DB, includeMemStore bool, epoch time.Time, resolution time.Duration) {
	sqlString := fmt.Sprintf(`
SELECT *
FROM test_a
GROUP BY _, STRIDE(%v)
ORDER BY _time`, resolution*4)

	epoch = encoding.RoundTimeUp(epoch, resolution)
	expectedResult{
		expectedRow{
			epoch,
			map[string]interface{}{},
			map[string]float64{
				"_points": 2,
				"i":       11,
				"ii":      22,
				"iii":     121,
				"iv":      15,
				"biv":     10,
				"z":       0,
			},
		},
		expectedRow{
			epoch.Add(4 * resolution),
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
				"_points": 2,
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
			epoch.Add(4 * resolution),
			map[string]interface{}{},
			map[string]float64{
				"_points": 1,
				"i":       500,
			},
		},
	}.assert(t, db, sqlString, includeMemStore)
}

func testAggregateQuery(t *testing.T, db *DB, includeMemStore bool, now time.Time, epoch time.Time, resolution time.Duration, asOf time.Time, until time.Time, scalingFactor int) {
	log.Debugf("AsOf: %v  Until: %v", asOf, until)

	period := resolution * time.Duration(scalingFactor)

	sqlString := fmt.Sprintf(`
SELECT
	iii / 2 AS ciii,
	IF(b != true, ii) AS ii,
	biv / 10 AS biv,
	*,
	IF(b = true, i) AS i_filtered,
	_points,
	5.1 as cval
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
				"i_filtered": 0,
				"i":          31,
				"ii":         42,
				"iii":        float64(31*42) / float64(1),
				"iv":         0,
				"biv":        0,
				"ciii":       float64(31*42) / float64(1) / float64(2),
				"cval":       5.1,
				"z":          53,
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
				"i_filtered": 0,
				"i":          122,
				"ii":         244,
				"iii":        float64(122*244) / float64(3),
				"iv":         20,
				"biv":        1,
				"ciii":       float64(122*244) / float64(3) / float64(2),
				"cval":       5.1,
				"z":          0,
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
		assert.Equal(t, erow.vals[fieldName], v, "Row %d - mismatch on field %v", idx, fieldName)
	}
}
