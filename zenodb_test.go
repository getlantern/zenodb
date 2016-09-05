package zenodb

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
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

func TestIntegration(t *testing.T) {
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
  maxmemstorebytes: 1
  retentionperiod: 200ms
  sql: >
    SELECT
      SUM(i) AS i,
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

	db, err := NewDB(&DBOpts{
		Dir:         tmpDir,
		SchemaFile:  tmpFile.Name(),
		VirtualTime: true,
	})
	if !assert.NoError(t, err, "Unable to create DB") {
		return
	}

	schemaB := schemaA + `
view_a:
  view: true
  maxmemstorebytes: 1
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
		db.clock.Advance(now)
		time.Sleep(250 * time.Millisecond)
		for _, table := range []string{"test_a", "view_a"} {
			log.Debug(db.PrintTableStats(table))
		}
	}

	tab := db.getTable("test_a")
	// This shuffles around fields to make sure that we're reading them correctly
	// from the file stores.
	shuffleFields := func() {
		time.Sleep(100 * time.Millisecond)
		tab.Fields[0], tab.Fields[1], tab.Fields[2] = tab.Fields[1], tab.Fields[2], tab.Fields[0]
		time.Sleep(100 * time.Millisecond)
	}

	randBelowRes := func() time.Duration {
		return time.Duration(-1 * rand.Intn(int(resolution)))
	}

	db.Insert("inbound", &Point{
		Ts: now.Add(randBelowRes()),
		Dims: map[string]interface{}{
			"r": "A",
			"u": 1,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  1,
			"ii": 2,
		},
	})
	shuffleFields()

	// This should get excluded by the filter
	db.Insert("inbound", &Point{
		Ts: now.Add(randBelowRes()),
		Dims: map[string]interface{}{
			"r": "B",
			"u": 1,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  1,
			"ii": 2,
		},
	})
	shuffleFields()

	db.Insert("inbound", &Point{
		Ts: now.Add(randBelowRes()),
		Dims: map[string]interface{}{
			"r": "A",
			"u": 1,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  10,
			"ii": 20,
		},
	})
	shuffleFields()

	// Change the schema a bit
	newFields := make([]sql.Field, 0, len(tab.Fields)+1)
	newFields = append(newFields, sql.NewField("newfield", AVG("h")))
	for _, field := range tab.Fields {
		newFields = append(newFields, field)
	}
	tab.Fields = newFields

	advance(resolution)

	db.Insert("inbound", &Point{
		Ts: now.Add(randBelowRes()),
		Dims: map[string]interface{}{
			"r": "A",
			"u": 1,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  111,
			"ii": 222,
		},
	})
	shuffleFields()

	db.Insert("inbound", &Point{
		Ts: now.Add(randBelowRes()),
		Dims: map[string]interface{}{
			"r": "A",
			"u": 2,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  31,
			"ii": 42,
			"z":  53,
		},
	})
	shuffleFields()

	db.Insert("inbound", &Point{
		Ts: now.Add(randBelowRes()),
		Dims: map[string]interface{}{
			"r": "A",
			"u": 2,
			"b": true,
		},
		Vals: map[string]float64{
			"i":  30000,
			"ii": 40000,
		},
	})
	shuffleFields()

	query := func(table string, from time.Time, to time.Time, dim string, field string) (map[int][]float64, error) {
		filter, queryErr := goexpr.Binary("!=", goexpr.Param("b"), goexpr.Constant(true))
		if queryErr != nil {
			return nil, queryErr
		}
		fromOffset := from.Sub(now)
		toOffset := to.Sub(now)
		result := make(map[int][]float64, 0)
		q := &query{
			table:       table,
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
		stats, err := q.run(db)
		log.Debugf("Query stats - scanned: %d    filterpass: %d    datavalid: %d    intimerange: %d", stats.Scanned, stats.FilterPass, stats.DataValid, stats.InTimeRange)
		log.Debugf("Result: %v", result)
		return result, err
	}

	// Give archiver time to catch up
	time.Sleep(2 * time.Second)

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

	testAggregateQuery(t, db, now, epoch, resolution)
}

func testAggregateQuery(t *testing.T, db *DB, now time.Time, epoch time.Time, resolution time.Duration) {
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
WHERE b != true AND r IN (SELECT r FROM test_a GROUP BY r, period(50000h))
GROUP BY r, u, period(%v)
HAVING ii * 2 = 488 OR ii = 42
ORDER BY u DESC
`, epoch.Add(-1*resolution).Sub(now), epoch.Add(3*resolution).Sub(now), resolution*time.Duration(scalingFactor)))
	if !assert.NoError(t, err, "Unable to create SQL query") {
		return
	}

	result, err := aq.Run()
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
	fields := make([]string, 0, len(result.FieldNames))
	for _, field := range result.FieldNames {
		fields = append(fields, field)
	}
	assert.Equal(t, []string{"ciii", "ii", "newfield", "_points", "i", "iii", "z", "i_filtered"}, fields)

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
`, resolution*time.Duration(scalingFactor)))
	if !assert.NoError(t, err, "Unable to creat query") {
		return
	}
	result, err = aq.Run()
	if !assert.NoError(t, err, "Unable to run query") {
		return
	}
	assert.Len(t, result.Rows, 0)

	// Test defaults
	aq = db.Query(&sql.Query{
		From:       "test_a",
		Fields:     []sql.Field{sql.NewField("ii", SUM("ii"))},
		GroupByAll: true,
		AsOfOffset: epoch.Add(-1 * resolution).Sub(now),
	})

	result, err = aq.Run()
	if assert.NoError(t, err, "Unable to run query with defaults") {
		assert.Equal(t, []string{"b", "r", "u"}, result.GroupBy)
		assert.NotNil(t, result.Until)
	}

	testMissingField(t, db, epoch, resolution, now)
}

func testMissingField(t *testing.T, db *DB, epoch time.Time, resolution time.Duration, now time.Time) {
	defer func() {
		assert.NotNil(t, recover(), "Query after removing fields should have panicked")
	}()

	tab := db.getTable("test_a")
	for i, field := range tab.Fields {
		tab.Fields[i] = sql.NewField("_"+field.Name, field.Expr)
	}

	aq := db.Query(&sql.Query{
		From:       "test_a",
		Fields:     []sql.Field{sql.NewField("ii", SUM("ii"))},
		GroupByAll: true,
		AsOfOffset: epoch.Add(-1 * resolution).Sub(now),
	})

	aq.Run()
}
