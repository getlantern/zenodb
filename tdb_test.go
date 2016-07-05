package tdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/davecgh/go-spew/spew"
	"github.com/oxtoacart/bytemap"
	. "github.com/oxtoacart/tdb/expr"

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
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	resolution := time.Millisecond
	hotPeriod := 2 * resolution
	retentionPeriod := 100 * resolution
	db := NewDB(&DBOpts{
		Dir:       tmpDir,
		BatchSize: 1,
	})
	err = db.CreateTable("test_A", resolution, hotPeriod, retentionPeriod, map[string]Expr{
		"i":   SUM("i"),
		"ii":  SUM("ii"),
		"iii": AVG(MULT("i", "ii")),
	})
	if !assert.NoError(t, err, "Unable to create table") {
		return
	}

	// Create a view grouped by dim "u"
	err = db.CreateView("test_a", "view_a", resolution, hotPeriod, retentionPeriod, "u")
	if !assert.NoError(t, err, "Unable to create view") {
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
			stats := db.TableStats(table)
			log.Debugf("%v (%v)\tInserted Points: %d\tDropped Points: %d\tHot Keys: %d\tArchived Buckets: %d", table, db.Now(table).In(time.UTC), stats.InsertedPoints, stats.DroppedPoints, stats.HotKeys, stats.ArchivedBuckets)
		}
	}

	db.Insert("test_a", &Point{
		Ts: now,
		Dims: map[string]interface{}{
			"r": "reporter1",
			"u": 1,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  1,
			"ii": 2,
		},
	})

	db.Insert("test_a", &Point{
		Ts: now,
		Dims: map[string]interface{}{
			"r": "reporter1",
			"u": 1,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  10,
			"ii": 20,
		},
	})

	advance(resolution)

	db.Insert("test_a", &Point{
		Ts: now,
		Dims: map[string]interface{}{
			"r": "reporter1",
			"u": 1,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  111,
			"ii": 222,
		},
	})

	db.Insert("test_a", &Point{
		Ts: now,
		Dims: map[string]interface{}{
			"r": "reporter1",
			"u": 2,
			"b": false,
		},
		Vals: map[string]float64{
			"i":  31,
			"ii": 42,
		},
	})

	db.Insert("test_a", &Point{
		Ts: now,
		Dims: map[string]interface{}{
			"r": "reporter1",
			"u": 2,
			"b": true,
		},
		Vals: map[string]float64{
			"i":  30000,
			"ii": 40000,
		},
	})

	advance(hotPeriod * 10)

	query := func(table string, from time.Time, to time.Time, dim string, field string) (map[uint64][]float64, error) {
		filter, queryErr := govaluate.NewEvaluableExpression("!b")
		if queryErr != nil {
			return nil, queryErr
		}
		fromOffset := from.Sub(now)
		toOffset := to.Sub(now)
		result := make(map[uint64][]float64, 0)
		_, err = db.runQuery(&query{
			table:      table,
			fields:     []string{field},
			fromOffset: fromOffset,
			toOffset:   toOffset,
			filter:     filter,
			onValues: func(keybytes bytemap.ByteMap, resultField string, vals []float64) {
				key := keybytes.AsMap()
				log.Debugf("%v : %v : %v", key, field, vals)
				if field == resultField {
					result[key[dim].(uint64)] = vals
				}
			},
		})
		return result, err
	}

	result, err := query("Test_A", epoch.Add(-1*resolution), epoch, "u", "i")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{11}, result[1])
		}
	}

	result, err = query("Test_A", epoch.Add(-1*resolution), epoch, "u", "iii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{101}, result[1])
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

	log.Debugf("%v -> %v", result.From, result.To)
	if !assert.Len(t, result.Entries, 1, "Wrong number of entries, perhaps HAVING isn't working") {
		return
	}
	entry := result.Entries[0]
	if !assert.EqualValues(t, "reporter1", entry.Dims["r"], "Wrong dim, result may be sorted incorrectly") {
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
	assert.Equal(t, []string{"sum_ii", "count_ii", "avg_ii", "min_ii"}, result.FieldOrder)

	// Test defaults
	aq = db.Query("Test_A").
		Select("sum_ii", SUM("ii")).
		FromOffset(epoch.Add(-1 * resolution).Sub(now))

	result, err = aq.Run()
	if assert.NoError(t, err, "Unable to run query with defaults") {
		assert.Equal(t, []string{"b", "r", "u"}, result.Dims)
		assert.NotNil(t, result.To)
	}
}
