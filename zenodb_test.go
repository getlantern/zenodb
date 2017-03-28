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

func TestClusterPushdown(t *testing.T) {
	doTestCluster(t, []string{"r", "u"})
}

func TestClusterNoPushdown(t *testing.T) {
	doTestCluster(t, nil)
}

func doTestCluster(t *testing.T, partitionBy []string) {
	numPartitions := 1 + rand.Intn(10)

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

	advance(resolution)

	nextTS := now.Add(randAboveRes())
	advanceClock(nextTS)
	db.Insert("inbound",
		nextTS,
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

	nextTS = now.Add(randAboveRes())
	advanceClock(nextTS)
	db.Insert("inbound",
		nextTS,
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

	nextTS = now.Add(randAboveRes())
	advanceClock(nextTS)
	db.Insert("inbound",
		nextTS,
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
	asOf := nextTS.Add(time.Duration(1-scalingFactor) * resolution)
	until := nextTS.Add(resolution)

	nextTS = now.Add(5 * randAboveRes())
	advanceClock(nextTS)
	db.Insert("inbound",
		nextTS,
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
	table.iterate(table.Fields.Names(), true, func(dims bytemap.ByteMap, vals []encoding.Sequence) {
		log.Debugf("Dims: %v")
		for i, field := range table.Fields {
			log.Debugf("Table Dump %v : %v", field.Name, vals[i].String(field.Expr, table.Resolution))
		}
	})
	testAggregateQuery(t, db, now, epoch, resolution, asOf, until, scalingFactor, modifyTable)
}

func testAggregateQuery(t *testing.T, db *DB, now time.Time, epoch time.Time, resolution time.Duration, asOf time.Time, until time.Time, scalingFactor int, modifyTable func(string, func(*table))) {
	log.Debugf("AsOf: %v  Until: %v", asOf, until)

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
`, asOf.In(time.UTC).Format(time.RFC3339), until.In(time.UTC).Format(time.RFC3339), resolution*time.Duration(scalingFactor))

	var rows []*core.FlatRow
	source, err := db.Query(sqlString, false, nil, randomlyIncludeMemStore())
	if !assert.NoError(t, err, "Unable to plan SQL query") {
		return
	}

	var fields core.Fields
	err = source.Iterate(context.Background(), func(inFields core.Fields) {
		fields = inFields
	}, func(row *core.FlatRow) (bool, error) {
		rows = append(rows, row)
		return true, nil
	})
	if !assert.NoError(t, err, "Unable to plan SQL query") {
		return
	}

	md := MetaDataFor(source, fields)
	log.Debugf("%v -> %v", md.AsOf, md.Until)
	log.Debug(spew.Sdump(rows))
	if !assert.Len(t, rows, 2, "Wrong number of rows, perhaps HAVING isn't working") {
		return
	}
	if !assert.EqualValues(t, 2, rows[0].Key.Get("u"), "Wrong dim, result may be sorted incorrectly") {
		return
	}
	if !assert.EqualValues(t, 1, rows[1].Key.Get("u"), "Wrong dim, result may be sorted incorrectly") {
		return
	}
	// TODO: _having shouldn't bleed through like that
	assert.Equal(t, []string{"ciii", "ii", "biv", "_points", "i", "iii", "iv", "z", "i_filtered", "cval"}, md.FieldNames)

	fieldIdx := func(name string) int {
		for i, candidate := range md.FieldNames {
			if candidate == name {
				return i
			}
		}
		return -1
	}

	pointsIdx := fieldIdx("_points")
	iIdx := fieldIdx("i")
	iiIdx := fieldIdx("ii")
	ciiiIdx := fieldIdx("ciii")
	ivIdx := fieldIdx("iv")
	bivIdx := fieldIdx("biv")
	zIdx := fieldIdx("z")
	iFilteredIdx := fieldIdx("i_filtered")
	cvalIdx := fieldIdx("cval")

	assert.EqualValues(t, 3, rows[1].Values[pointsIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 0, rows[1].Values[iFilteredIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 122, rows[1].Values[iIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 244, rows[1].Values[iiIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 20, rows[1].Values[ivIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 1, rows[1].Values[bivIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, float64(122*244)/float64(3)/float64(2), rows[1].Values[ciiiIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 5.1, rows[1].Values[cvalIdx])

	assert.EqualValues(t, 1, rows[0].Values[pointsIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 0, rows[0].Values[iFilteredIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 31, rows[0].Values[iIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 42, rows[0].Values[iiIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 0, rows[0].Values[ivIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 0, rows[0].Values[bivIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 53, rows[0].Values[zIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, float64(31*42)/float64(1)/float64(2), rows[0].Values[ciiiIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 5.1, rows[0].Values[cvalIdx])
}

func randomlyIncludeMemStore() bool {
	return rand.Float64() > 0.5
}
