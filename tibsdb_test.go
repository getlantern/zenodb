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
    GROUP BY period(1ms)
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
  maxmemstorebytes: 1
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

	log.Debug("A")
	result, err := query("Test_A", epoch.Add(-1*resolution), epoch, "u", "i")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{11}, result[1])
		}
	}

	log.Debug("B")
	result, err = query("Test_A", epoch.Add(-1*resolution), epoch, "u", "ii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{22}, result[1])
		}
	}

	log.Debug("C")
	result, err = query("Test_A", epoch.Add(-1*resolution), epoch, "u", "iii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 1) {
			assert.Equal(t, []float64{121}, result[1])
		}
	}

	log.Debug("D")
	result, err = query("Test_A", epoch.Add(-2*resolution), epoch.Add(resolution*2), "u", "ii")
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 2) {
			assert.Equal(t, []float64{222, 22}, result[1])
			assert.Equal(t, []float64{42}, result[2])
		}
	}

	log.Debug("E")
	result, err = query("Test_A", epoch.Add(-2*resolution), epoch.Add(resolution*2), "u", "ii")
	log.Debug(result)
	if assert.NoError(t, err, "Unable to run query") {
		if assert.Len(t, result, 2) {
			assert.Equal(t, []float64{222, 22}, result[1])
			assert.Equal(t, []float64{42}, result[2])
		}
	}

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
GROUP BY r, period('%v')
HAVING ii * 2 = 572
ORDER BY ciii DESC
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
	if !assert.Equal(t, 1, result.NumPeriods, "Wrong number of periods, bucketing may not be working correctly") {
		return
	}
	log.Debug(entry.Dims)
	log.Debug(result.NumPeriods)
	log.Debugf("i: %v", entry.Fields[2].String(aq.Fields[2].Expr))
	log.Debugf("ii: %v", entry.Fields[1].String(aq.Fields[1].Expr))
	log.Debugf("iii: %v", entry.Fields[0].String(aq.Fields[0].Expr))
	i, _ := entry.Fields[2].ValueAt(0, aq.Fields[2].Expr)
	ii, _ := entry.Fields[1].ValueAt(0, aq.Fields[1].Expr)
	iii, _ := entry.Fields[0].ValueAt(0, aq.Fields[0].Expr)
	assert.EqualValues(t, 153, i, "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, 286, ii, "Wrong derived value, bucketing may not be working correctly")
	assert.EqualValues(t, float64(153*286)/float64(4)/float64(2), iii, "Wrong derived value, bucketing may not be working correctly")
	fields := make([]string, 0, len(result.Fields))
	for _, field := range result.Fields {
		fields = append(fields, field.Name)
	}
	assert.Equal(t, []string{"ciii", "ii", "i"}, fields)

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
