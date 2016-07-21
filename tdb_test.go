package tdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/davecgh/go-spew/spew"
	"github.com/getlantern/bytemap"
	. "github.com/getlantern/tdb/expr"
	"github.com/getlantern/tdb/sql"

	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRoundTime(t *testing.T) {
	ts := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	rounded := roundTime(ts, time.Second)
	expected := time.Date(2015, 5, 6, 7, 8, 9, 0, time.UTC)
	assert.Equal(t, expected, rounded)
}

func TestIntegration(t *testing.T) {
	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC)

	tmpDir, err := ioutil.TempDir("", "tdbtest")
	if !assert.NoError(t, err, "Unable to create temp directory") {
		return
	}
	defer os.RemoveAll(tmpDir)

	tmpFile, err := ioutil.TempFile("", "tdbschema")
	if !assert.NoError(t, err, "Unable to create temp file") {
		return
	}
	tmpFile.Close()

	resolution := time.Millisecond

	schemaA := `
Test_a:
  retentionperiod: 200ms
  sql: >
    SELECT
      SUM(i) AS i,
      SUM(ii) AS ii,
      SUM(i) * SUM(ii) / COUNT(ii) AS iii
    FROM inbound
    WHERE r = 'A'
    GROUP BY period(1ms)
`
	err = ioutil.WriteFile(tmpFile.Name(), []byte(schemaA), 0644)
	if !assert.NoError(t, err, "Unable to write schemaA") {
		return
	}

	db, err := NewDB(&DBOpts{
		Dir:        tmpDir,
		BatchSize:  1,
		SchemaFile: tmpFile.Name(),
	})
	if !assert.NoError(t, err, "Unable to create DB") {
		return
	}

	schemaB := schemaA + `
view_a:
  hotperiod: 2ms
  retentionperiod: 200ms
  sql: >
    SELECT
      SUM(i) AS i,
      SUM(ii) AS ii,
      AVG(i) * AVG(ii) AS iii
    FROM inbound
    WHERE r = 'A'
    GROUP BY u, period(1ms)`
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
		db.getTable("test_a").clock.Advance(now)
		db.getTable("view_a").clock.Advance(now)
		time.Sleep(250 * time.Millisecond)
		for _, table := range []string{"test_a", "view_a"} {
			log.Debug(db.PrintTableStats(table))
		}
	}

	db.Insert("inbound", &Point{
		Ts: now,
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

	// This should get excluded by the filter
	db.Insert("inbound", &Point{
		Ts: now,
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

	db.Insert("inbound", &Point{
		Ts: now,
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

	advance(resolution)

	db.Insert("inbound", &Point{
		Ts: now,
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

	db.Insert("inbound", &Point{
		Ts: now,
		Dims: map[string]interface{}{
			"r": "A",
			"u": 2,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  31,
			"ii": 42,
		},
	})

	db.Insert("inbound", &Point{
		Ts: now,
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

	query := func(table string, from time.Time, to time.Time, dim string, field string) (map[int][]float64, error) {
		filter, queryErr := govaluate.NewEvaluableExpression("!b")
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
			onValues: func(keybytes bytemap.ByteMap, resultField string, vals []float64) {
				key := keybytes.AsMap()
				log.Debugf("%v : %v : %v", key, field, vals)
				if field == resultField {
					result[key[dim].(int)] = vals
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

	result, err := query("Test_A", epoch.Add(-1*resolution), epoch, "u", "i")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{11}, result[1])
		}
	}

	result, err = query("Test_A", epoch.Add(-1*resolution), epoch, "u", "ii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{22}, result[1])
		}
	}

	result, err = query("Test_A", epoch.Add(-1*resolution), epoch, "u", "iii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{121}, result[1])
		}
	}

	result, err = query("Test_A", epoch.Add(-2*resolution), epoch.Add(resolution*2), "u", "ii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 2) {
			assert.Equal(t, []float64{222, 22, 0, 0}, result[1])
			assert.Equal(t, []float64{42, 0, 0, 0}, result[2])
		}
	}

	result, err = query("Test_A", epoch.Add(-2*resolution), epoch.Add(resolution*2), "u", "ii")
	log.Debug(result)
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 2) {
			assert.Equal(t, []float64{222, 22, 0, 0}, result[1])
			assert.Equal(t, []float64{42, 0, 0, 0}, result[2])
		}
	}

	testAggregateQuery(t, db, now, epoch, resolution)
}

func testAggregateQuery(t *testing.T, db *DB, now time.Time, epoch time.Time, resolution time.Duration) {
	scalingFactor := 5

	aq, err := db.SQLQuery(fmt.Sprintf(`
SELECT
	SUM(ii) AS sum_ii,
	COUNT(ii) AS count_ii,
	AVG(ii) * 2 AS avg_ii,
	MIN(ii) AS min_ii
FROM test_a
ASOF '%v' UNTIL '%v'
WHERE b != true
GROUP BY r, period('%v')
HAVING SUM(sum_ii) = 286
ORDER BY AVG(avg_ii) DESC
`, epoch.Add(-1*resolution).Sub(now), epoch.Add(3*resolution).Sub(now), resolution*time.Duration(scalingFactor)))
	if !assert.NoError(t, err, "Unable to create SQL query") {
		return
	}

	result, err := aq.Run()
	if !assert.NoError(t, err, "Unable to run query") {
		return
	}

	log.Debugf("%v -> %v", result.AsOf, result.Until)
	if !assert.Len(t, result.Entries, 1, "Wrong number of entries, perhaps HAVING isn't working") {
		return
	}
	entry := result.Entries[0]
	if !assert.EqualValues(t, "A", entry.Dims["r"], "Wrong dim, result may be sorted incorrectly") {
		return
	}
	if !assert.Len(t, entry.Fields["avg_ii"], 1, "Wrong number of periods, bucketing may not be working correctly") {
		return
	}
	avg := float64(286) / float64(scalingFactor)
	assert.EqualValues(t, 286, entry.Fields["sum_ii"][0].Get(), "Wrong derived value, bucketing may not be working correctly")
	if !assert.EqualValues(t, avg, entry.Fields["avg_ii"][0].Get(), "Wrong derived value, bucketing may not be working correctly") {
		t.Log(spew.Sprint(entry.Fields["avg_ii"][0]))
	}
	assert.EqualValues(t, 0, entry.Fields["min_ii"][0].Get(), "Wrong derived value, bucketing may not be working correctly")
	fields := make([]string, 0, len(result.Fields))
	for _, field := range result.Fields {
		fields = append(fields, field.Name)
	}
	assert.Equal(t, []string{"sum_ii", "count_ii", "avg_ii", "min_ii"}, fields)

	// Test defaults
	aq = db.Query(&sql.Query{
		From:       "test_a",
		Fields:     []sql.Field{sql.Field{Expr: SUM("ii"), Name: "sum_ii"}},
		AsOfOffset: epoch.Add(-1 * resolution).Sub(now),
	})

	result, err = aq.Run()
	if assert.NoError(t, err, "Unable to run query with defaults") {
		assert.Equal(t, []string{"b", "r", "u"}, result.GroupBy)
		assert.NotNil(t, result.Until)
	}
}
