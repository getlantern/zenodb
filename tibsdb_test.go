package tibsdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/bytemap"
	. "github.com/getlantern/tibsdb/expr"
	"github.com/getlantern/tibsdb/sql"
	"github.com/getlantern/vtime"

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
	originalClock := clock
	defer func() {
		clock = originalClock
	}()

	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC)
	clock = vtime.NewVirtualClock(time.Time{})

	tmpDir, err := ioutil.TempDir("", "tibsdbtest")
	if !assert.NoError(t, err, "Unable to create temp directory") {
		return
	}
	defer os.RemoveAll(tmpDir)

	tmpFile, err := ioutil.TempFile("", "tibsdbschema")
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
      i * ii / COUNT(ii) AS iii
    FROM inbound
    WHERE r = 'A'
    GROUP BY *, period(1ms)
`
	err = ioutil.WriteFile(tmpFile.Name(), []byte(schemaA), 0644)
	if !assert.NoError(t, err, "Unable to write schemaA") {
		return
	}

	db, err := NewDB(&DBOpts{
		Dir:        tmpDir,
		SchemaFile: tmpFile.Name(),
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
    SELECT
      i,
      ii,
      iii
    FROM teSt_a
    WHERE r = 'A'
    GROUP BY u, b, period(1ms)`
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
		clock.Advance(now)
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
		filter, queryErr := govaluate.NewEvaluableExpression("b != true")
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
			onValues: func(keybytes bytemap.ByteMap, resultField string, e Expr, seq sequence, startOffset int) {
				key := keybytes.AsMap()
				log.Debugf("%v : %v : %v : %d : %v", key, field, resultField, startOffset, seq.String(e))
				if field == resultField {
					numPeriods := seq.numPeriods(e.EncodedWidth())
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
	test(epoch.Add(-2*resolution), epoch.Add(resolution*2), "u", "ii", func(result map[int][]float64) {
		log.Debug(result)
		if assert.Len(t, result, 2) {
			assert.Equal(t, []float64{222, 22}, result[1])
			assert.Equal(t, []float64{42}, result[2])
		}
	})

	testAggregateQuery(t, db, now, epoch, resolution)
}

func testAggregateQuery(t *testing.T, db *DB, now time.Time, epoch time.Time, resolution time.Duration) {
	scalingFactor := 5

	aq, err := db.SQLQuery(fmt.Sprintf(`
SELECT
	iii / 2 AS ciii,
	ii,
	i
FROM test_a
ASOF '%v' UNTIL '%v'
WHERE b != true
GROUP BY r, u, period('%v')
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
	assert.Equal(t, []string{"ciii", "ii", "i"}, fields)

	assert.EqualValues(t, 122, rows[1].Values[2], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 244, rows[1].Values[1], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, float64(122*244)/float64(3)/float64(2), rows[1].Values[0], "Wrong derived value, bucketing may not be working correctly")

	assert.EqualValues(t, 31, rows[0].Values[2], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 42, rows[0].Values[1], "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, float64(31*42)/float64(1)/float64(2), rows[0].Values[0], "Wrong derived value, bucketing may not be working correctly")

	// Test defaults
	aq = db.Query(&sql.Query{
		From:       "test_a",
		Fields:     []sql.Field{sql.Field{Expr: SUM("ii"), Name: "ii"}},
		GroupByAll: true,
		AsOfOffset: epoch.Add(-1 * resolution).Sub(now),
	})

	result, err = aq.Run()
	if assert.NoError(t, err, "Unable to run query with defaults") {
		assert.Equal(t, []string{"b", "r", "u"}, result.GroupBy)
		assert.NotNil(t, result.Until)
	}
}
