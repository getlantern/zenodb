package tdb

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
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

func TestRoundTrip(t *testing.T) {
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
		"i":   Sum("i"),
		"ii":  Sum("ii"),
		"iii": Avg(Mult("i", "ii")),
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
			"b": true,
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
			"b": true,
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
			"b": true,
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

	advance(hotPeriod * 10)

	query := func(table string, from time.Time, to time.Time, dim string, field string) (map[uint64][]float64, error) {
		result := make(map[uint64][]float64, 0)
		err = db.RunQuery(&Query{
			Table:  table,
			Fields: []string{field},
			From:   from,
			To:     to,
			OnValues: func(key map[string]interface{}, resultField string, vals []float64) {
				log.Debugf("%v : %v : %v", key, field, vals)
				if field == resultField {
					result[key[dim].(uint64)] = vals
				}
			},
		})
		return result, err
	}

	result, err := query("Test_A", epoch, epoch, "u", "i")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{11}, result[1])
		}
	}

	result, err = query("Test_A", epoch, epoch, "u", "iii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{101}, result[1])
		}
	}

	result, err = query("Test_A", epoch.Add(-1*resolution), epoch.Add(resolution*2), "u", "ii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 2) {
			assert.Equal(t, []float64{222, 22, 0, 0}, result[1])
			assert.Equal(t, []float64{42, 0, 0, 0}, result[2])
		}
	}

	result, err = query("Test_A", epoch.Add(-1*resolution), epoch.Add(resolution*2), "u", "ii")
	log.Debug(result)
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 2) {
			assert.Equal(t, []float64{222, 22, 0, 0}, result[1])
			assert.Equal(t, []float64{42, 0, 0, 0}, result[2])
		}
	}

	testAggregateQuery(t, db, epoch, resolution)
}

func testAggregateQuery(t *testing.T, db *DB, epoch time.Time, resolution time.Duration) {
	scalingFactor := 5

	aq := Aggregate("Test_A", resolution*time.Duration(scalingFactor)).
		Select("sum_ii", Sum("ii")).
		Select("count_ii", Count("ii")).
		Select("avg_ii", Avg("ii")).
		Select("min_ii", Min("ii")).
		GroupBy("r").
		OrderBy("avg_ii", false).
		From(epoch.Add(-1 * resolution)).
		To(epoch.Add(resolution * 2))

	result, err := aq.Run(db)
	if assert.NoError(t, err, "Unable to run query") {
		if assert.EqualValues(t, "reporter1", result[0].Dims["r"], "Wrong dim, result may be sorted incorrectly") {
			if assert.Len(t, result[0].Fields["avg_ii"], 1, "Wrong number of periods, bucketing may not be working correctly") {
				avg := float64(286) / 2 / float64(scalingFactor)
				assert.EqualValues(t, 286, result[0].Fields["sum_ii"][0].Get(), "Wrong derived value, bucketing may not be working correctly")
				if !assert.EqualValues(t, avg, result[0].Fields["avg_ii"][0].Get(), "Wrong derived value, bucketing may not be working correctly") {
					t.Log(spew.Sprint(result[0].Fields["avg_ii"][0]))
				}
				assert.EqualValues(t, 0, result[0].Fields["min_ii"][0].Get(), "Wrong derived value, bucketing may not be working correctly")
				assert.EqualValues(t, 286, result[0].Totals["sum_ii"].Get(), "Wrong total value")
				if !assert.EqualValues(t, avg, result[0].Totals["avg_ii"].Get(), "Wrong total value") {
					t.Log(spew.Sprint(result[0].Totals["avg_ii"]))
				}
			}
		}
	}
}
