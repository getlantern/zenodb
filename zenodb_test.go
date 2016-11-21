package zenodb

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/encoding"
	. "github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/sql"

	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRoundTime(t *testing.T) {
	ts := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	rounded := encoding.RoundTime(ts, time.Second)
	expected := time.Date(2015, 5, 6, 7, 8, 10, 0, time.UTC)
	assert.Equal(t, expected, rounded)
}

func TestSingleDB(t *testing.T) {
	doTest(t, false, func(tmpDir string, tmpFile string) (*DB, func(time.Time), func(string, func(*table))) {
		db, err := NewDB(&DBOpts{
			Dir:              filepath.Join(tmpDir, "leader"),
			SchemaFile:       tmpFile,
			VirtualTime:      true,
			MaxMemStoreBytes: 1,
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

func TestCluster(t *testing.T) {
	numPartitions := 1 + rand.Intn(10)
	numPartitions = 1

	doTest(t, true, func(tmpDir string, tmpFile string) (*DB, func(time.Time), func(string, func(*table))) {
		leader, err := NewDB(&DBOpts{
			Dir:              filepath.Join(tmpDir, "leader"),
			SchemaFile:       tmpFile,
			VirtualTime:      true,
			Passthrough:      true,
			NumPartitions:    numPartitions,
			MaxMemStoreBytes: 1,
		})
		if !assert.NoError(t, err, "Unable to create leader DB") {
			t.Fatal()
		}

		followers := make([]*DB, 0, numPartitions)
		for i := 0; i < numPartitions; i++ {
			part := i
			follower, followerErr := NewDB(&DBOpts{
				Dir:              filepath.Join(tmpDir, fmt.Sprintf("follower%d", i)),
				SchemaFile:       tmpFile,
				VirtualTime:      true,
				Partition:        part,
				MaxMemStoreBytes: 1,
				Follow: func(f *Follow, cb func(data []byte, newOffset wal.Offset) error) {
					leader.Follow(f, cb)
				},
				RegisterRemoteQueryHandler: func(partition int, query QueryFN) {
					var register func()
					register = func() {
						leader.RegisterQueryHandler(partition, func(sqlString string, includeMemStore bool, isSubQuery bool, subQueryResults [][]interface{}, onValue func(dims bytemap.ByteMap, vals []encoding.Sequence)) (bool, error) {
							// Re-register when finished
							defer register()

							hasReadResult := false
							err := query(sqlString, includeMemStore, isSubQuery, subQueryResults, func(entry *Entry) error {
								onValue(entry.Dims, entry.Vals)
								hasReadResult = true
								return nil
							})
							return hasReadResult, err
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

func doTest(t *testing.T, isClustered bool, buildDB func(tmpDir string, tmpFile string) (*DB, func(time.Time), func(string, func(*table)))) {
	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC)

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

	resolution := time.Millisecond

	schemaA := `
Test_a:
  maxflushlatency: 1ms
  retentionperiod: 200ms
  sql: >
    SELECT
      IF(md = 'glub', SUM(i)) AS i,
      ii,
      i * ii / COUNT(ii) AS iii,
      z
    FROM inbound
    WHERE r = 'A'
    GROUP BY *, period(1ms)
`
	err = ioutil.WriteFile(tmpFile.Name(), []byte(schemaA), 0644)
	if !assert.NoError(t, err, "Unable to write schemaA") {
		return
	}

	db, advanceClock, modifyTable := buildDB(tmpDir, tmpFile.Name())

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
		modifyTable("test_a", func(tab *table) {
			tab.Fields[0], tab.Fields[1], tab.Fields[2] = tab.Fields[1], tab.Fields[2], tab.Fields[0]
		})
		time.Sleep(100 * time.Millisecond)
	}

	randBelowRes := func() time.Duration {
		return time.Duration(-1 * rand.Intn(int(resolution)))
	}

	db.Insert("inbound",
		now.Add(randBelowRes()),
		map[string]interface{}{
			"r":  "A",
			"u":  1,
			"b":  false,
			"md": "glub",
		},
		map[string]float64{
			"i":  1,
			"ii": 2,
		})
	shuffleFields()

	// This should get excluded by the filter
	db.Insert("inbound",
		now.Add(randBelowRes()),
		map[string]interface{}{
			"r":  "B",
			"u":  1,
			"b":  false,
			"md": "glub",
		},
		map[string]float64{
			"i":  1,
			"ii": 2,
		})
	shuffleFields()

	db.Insert("inbound",
		now.Add(randBelowRes()),
		map[string]interface{}{
			"r":  "A",
			"u":  1,
			"b":  false,
			"md": "glub",
		},
		map[string]float64{
			"i":  10,
			"ii": 20,
		})
	shuffleFields()

	// Change the schema a bit
	modifyTable("test_a", func(tab *table) {
		newFields := make([]sql.Field, 0, len(tab.Fields)+1)
		newFields = append(newFields, sql.NewField("newfield", AVG("h")))
		for _, field := range tab.Fields {
			newFields = append(newFields, field)
		}
		tab.Fields = newFields
	})

	advance(resolution)

	nextTS := now.Add(randBelowRes())
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
		})
	shuffleFields()

	nextTS = now.Add(randBelowRes())
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

	nextTS = now.Add(randBelowRes())
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

	// Give archiver time to catch up
	time.Sleep(2 * time.Second)

	if !isClustered {
		testQueries(t, epoch, resolution, now, db)
	}

	testAggregateQuery(t, db, now, epoch, resolution, modifyTable)
}

func testQueries(t *testing.T, epoch time.Time, resolution time.Duration, now time.Time, db *DB) {
	query := func(table string, from time.Time, to time.Time, dim string, field string) (map[int][]float64, error) {
		filter, queryErr := goexpr.Binary("!=", goexpr.Param("b"), goexpr.Constant(true))
		if queryErr != nil {
			return nil, queryErr
		}
		fromOffset := from.Sub(now)
		toOffset := to.Sub(now)
		result := make(map[int][]float64, 0)
		q := &query{
			t:           db.getTable(table),
			fields:      []string{field},
			asOfOffset:  fromOffset,
			untilOffset: toOffset,
			filter:      filter,
			onValues: func(keybytes bytemap.ByteMap, resultField string, e Expr, seq encoding.Sequence, startOffset int) {
				key := keybytes.AsMap()
				log.Debugf("%v : %v : %v : %d : %v", key, field, resultField, startOffset, seq.String(e))
				if field == resultField {
					numPeriods := seq.NumPeriods(e.EncodedWidth())
					vals := make([]float64, 0, numPeriods-startOffset)
					for i := 0; i < numPeriods-startOffset; i++ {
						val, wasSet := seq.ValueAt(i+startOffset, e)
						if wasSet {
							vals = append(vals, val)
						}
					}
					if len(vals) > 0 {
						result[key[dim].(int)] = vals
					}
				}
			},
		}
		err := q.init(db)
		if err != nil {
			return nil, err
		}
		stats, err := q.run(db, randomlyIncludeMemStore())
		log.Debugf("Query stats - scanned: %d    filterpass: %d    datavalid: %d    intimerange: %d", stats.Scanned, stats.FilterPass, stats.DataValid, stats.InTimeRange)
		log.Debugf("Result: %v", result)
		return result, err
	}

	test := func(from time.Time, to time.Time, dim string, field string, onResult func(map[int][]float64)) {
		for _, table := range []string{"Test_A", "view_A"} {
			result, err := query(table, from, to, dim, field)
			if assert.NoError(t, err, "Unable to run query") {
				t.Logf("Table: %v", table)
				onResult(result)
			}
		}
	}

	log.Debug("A")
	test(epoch.Add(-1*resolution), epoch, "u", "i", func(result map[int][]float64) {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{11}, result[1])
		}
	})

	log.Debug("B")
	test(epoch.Add(-1*resolution), epoch, "u", "ii", func(result map[int][]float64) {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{22}, result[1])
		}
	})

	log.Debug("C")
	test(epoch.Add(-1*resolution), epoch, "u", "iii", func(result map[int][]float64) {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{121}, result[1])
		}
	})

	log.Debug("D")
	test(epoch.Add(-2*resolution), epoch.Add(resolution*2), "u", "ii", func(result map[int][]float64) {
		if assert.Len(t, result, 2) {
			assert.Equal(t, []float64{222, 22}, result[1])
			assert.Equal(t, []float64{42}, result[2])
		}
	})

	log.Debug("E")
	test(epoch.Add(-2*resolution), epoch.Add(resolution*2), "u", "z", func(result map[int][]float64) {
		log.Debug(result)
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{53}, result[2])
		}
	})

}

func testAggregateQuery(t *testing.T, db *DB, now time.Time, epoch time.Time, resolution time.Duration, modifyTable func(string, func(*table))) {
	scalingFactor := 5

	aq, err := db.SQLQuery(fmt.Sprintf(`
SELECT
	iii / 2 AS ciii,
	IF(b != true, ii) AS ii,
	*,
	IF(b = true, i) AS i_filtered,
	_points
FROM test_a
ASOF '%v' UNTIL '%v'
WHERE b != true AND r IN (SELECT r FROM test_a)
GROUP BY r, u, period(%v)
HAVING ii * 2 = 488 OR ii = 42 OR unknown = 12
ORDER BY u DESC
`, epoch.Add(-1*resolution).Sub(now), epoch.Add(3*resolution).Sub(now), resolution*time.Duration(scalingFactor)), randomlyIncludeMemStore())
	if !assert.NoError(t, err, "Unable to create SQL query") {
		return
	}

	result, err := aq.Run(false)
	if !assert.NoError(t, err, "Unable to run query") {
		return
	}

	log.Debugf("%v -> %v", result.AsOf, result.Until)
	if !assert.Equal(t, 1, result.NumPeriods, "Wrong number of periods, bucketing may not be working correctly") {
		return
	}
	rows := result.Rows
	log.Debug(spew.Sdump(result.Rows))
	if !assert.Len(t, rows, 2, "Wrong number of rows, perhaps HAVING isn't working") {
		return
	}
	if !assert.EqualValues(t, 2, result.Rows[0].Dims[1], "Wrong dim, result may be sorted incorrectly") {
		return
	}
	if !assert.EqualValues(t, 1, result.Rows[1].Dims[1], "Wrong dim, result may be sorted incorrectly") {
		return
	}
	assert.Equal(t, []string{"ciii", "ii", "newfield", "_points", "i", "iii", "z", "i_filtered"}, result.FieldNames)

	pointsIdx := 3
	iIdx := 4
	iiIdx := 1
	ciiiIdx := 0
	zIdx := 6
	iFilteredIdx := 7

	assert.EqualValues(t, 3, rows[1].Values[pointsIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 0, rows[1].Values[iFilteredIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 122, rows[1].Values[iIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 244, rows[1].Values[iiIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, float64(122*244)/float64(3)/float64(2), rows[1].Values[ciiiIdx], "Wrong derived value, bucketing may not be working correctly")

	assert.EqualValues(t, 3, rows[1].Values[pointsIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 0, rows[0].Values[iFilteredIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 31, rows[0].Values[iIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 42, rows[0].Values[iiIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 53, rows[0].Values[zIdx], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, float64(31*42)/float64(1)/float64(2), rows[0].Values[ciiiIdx], "Wrong derived value, bucketing may not be working correctly")

	// Test having on non-existent field
	aq, err = db.SQLQuery(fmt.Sprintf(`
SELECT i
FROM test_a
GROUP BY period('%v')
HAVING unknown = 5
`, resolution*time.Duration(scalingFactor)), randomlyIncludeMemStore())
	if !assert.NoError(t, err, "Unable to create query") {
		return
	}
	result, err = aq.Run(false)
	if !assert.NoError(t, err, "Unable to run query") {
		return
	}
	assert.Len(t, result.Rows, 0)

	// Test defaults
	aq = db.Query(&sql.Query{
		From:           "test_a",
		Fields:         []sql.Field{sql.NewField("ii", SUM("ii"))},
		IncludedFields: []string{"_points", "ii"},
		GroupByAll:     true,
		AsOfOffset:     epoch.Add(-1 * resolution).Sub(now),
	}, randomlyIncludeMemStore())

	result, err = aq.Run(false)
	if assert.NoError(t, err, "Unable to run query with defaults") {
		assert.Equal(t, []string{"b", "md", "r", "u"}, result.GroupBy)
		assert.NotNil(t, result.Until)
	}

	testMissingField(t, db, epoch, resolution, now, modifyTable)
}

func testMissingField(t *testing.T, db *DB, epoch time.Time, resolution time.Duration, now time.Time, modifyTable func(string, func(*table))) {
	var err error
	defer func() {
		assert.True(t, err != nil || recover() != nil, "Query after removing fields should have panicked")
	}()

	modifyTable("test_a", func(tab *table) {
		for i, field := range tab.Fields {
			tab.Fields[i] = sql.NewField("_"+field.Name, field.Expr)
		}
	})

	aq := db.Query(&sql.Query{
		From:       "test_a",
		Fields:     []sql.Field{sql.NewField("ii", SUM("ii"))},
		GroupByAll: true,
		AsOfOffset: epoch.Add(-1 * resolution).Sub(now),
	}, randomlyIncludeMemStore())

	_, err = aq.Run(false)
}

func randomlyIncludeMemStore() bool {
	return rand.Float64() > 0.5
}
