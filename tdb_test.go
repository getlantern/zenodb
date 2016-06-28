package tdb

import (
	"io/ioutil"
	"os"
	"time"

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
	err = db.CreateTable("test_A", resolution, hotPeriod, retentionPeriod, DerivedField{
		Name: "iii",
		Expr: Avg(Calc("i*ii")),
	})
	if !assert.NoError(t, err, "Unable to create table") {
		return
	}

	now := epoch
	advance := func(d time.Duration) {
		time.Sleep(250 * time.Millisecond)
		now = now.Add(d)
		db.getTable("test_a").clock.Advance(now)
		time.Sleep(250 * time.Millisecond)
		stats := db.TableStats("test_a")
		log.Debugf("At %v\tInserted Points: %d\tDropped Points: %d\tHot Keys: %d\tArchived Buckets: %d", now, stats.InsertedPoints, stats.DroppedPoints, stats.HotKeys, stats.ArchivedBuckets)
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

	query := func(from time.Time, to time.Time, dim string, field string) (map[uint64][]float64, error) {
		result := make(map[uint64][]float64, 0)
		err = db.RunQuery(&Query{
			Table:  "Test_A",
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

	result, err := query(epoch, epoch, "u", "i")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{11}, result[1])
		}
	}

	result, err = query(epoch, epoch, "u", "iii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{101}, result[1])
		}
	}

	result, err = query(epoch.Add(-1*resolution), epoch.Add(resolution*2), "u", "ii")
	log.Debug(result)
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 2) {
			assert.Equal(t, []float64{222, 22, 0, 0}, result[1])
			assert.Equal(t, []float64{42, 0, 0, 0}, result[2])
		}
	}
}
