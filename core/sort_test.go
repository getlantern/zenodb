package core

import (
	"sort"
	"testing"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
)

func TestSortTime(t *testing.T) {
	rows := sortedRows(false, "_time")
	assert.Equal(t, []int64{0, 1, 2, 3, 4, 5}, actualTimes(rows))

	rows = sortedRows(true, "_time")
	assert.Equal(t, []int64{5, 4, 3, 2, 1, 0}, actualTimes(rows))
}

func TestSortBools(t *testing.T) {
	rows := sortedRows(false, "b")
	assert.Equal(t, []bool{false, false, false, true, true, true}, actualBools(rows))

	rows = sortedRows(true, "b")
	assert.Equal(t, []bool{true, true, true, false, false, false}, actualBools(rows))
}

func TestSortString(t *testing.T) {
	rows := sortedRows(false, "s")
	assert.Equal(t, []string{"", "a", "b", "b", "c", "c"}, actualStrings(rows))

	rows = sortedRows(true, "s")
	assert.Equal(t, []string{"c", "c", "b", "b", "a", ""}, actualStrings(rows))
}

func TestSortVals(t *testing.T) {
	rows := sortedRows(false, "val")
	assert.Equal(t, []float64{0, 12, 23, 56, 56, 78}, actualVals(rows))

	rows = sortedRows(true, "val")
	assert.Equal(t, []float64{78, 56, 56, 23, 12, 0}, actualVals(rows))
}

func TestSortAll(t *testing.T) {
	rows := sortedRows(false, "b", "s", "val", "_time")
	assert.Equal(t, []bool{false, false, false, true, true, true}, actualBools(rows))
	assert.Equal(t, []string{"a", "b", "b", "", "c", "c"}, actualStrings(rows))
	assert.Equal(t, []float64{78, 0, 23, 12, 56, 56}, actualVals(rows))
	assert.Equal(t, []int64{1, 5, 2, 4, 0, 3}, actualTimes(rows))

	rows = sortedRows(true, "b", "s", "val", "_time")
	assert.Equal(t, []bool{true, true, true, false, false, false}, actualBools(rows))
	assert.Equal(t, []string{"c", "c", "", "b", "b", "a"}, actualStrings(rows))
	assert.Equal(t, []float64{56, 56, 12, 23, 0, 78}, actualVals(rows))
	assert.Equal(t, []int64{3, 0, 4, 2, 5, 1}, actualTimes(rows))
}

func actualTimes(rows []*FlatRow) []int64 {
	return []int64{rows[0].TS, rows[1].TS, rows[2].TS, rows[3].TS, rows[4].TS, rows[5].TS}
}

func actualStrings(rows []*FlatRow) []string {
	return []string{rows[0].Key.Get("s").(string), rows[1].Key.Get("s").(string), rows[2].Key.Get("s").(string), rows[3].Key.Get("s").(string), rows[4].Key.Get("s").(string), rows[5].Key.Get("s").(string)}
}

func actualBools(rows []*FlatRow) []bool {
	return []bool{rows[0].Key.Get("b").(bool), rows[1].Key.Get("b").(bool), rows[2].Key.Get("b").(bool), rows[3].Key.Get("b").(bool), rows[4].Key.Get("b").(bool), rows[5].Key.Get("b").(bool)}
}

func actualVals(rows []*FlatRow) []float64 {
	return []float64{rows[0].Values[0], rows[1].Values[0], rows[2].Values[0], rows[3].Values[0], rows[4].Values[0], rows[5].Values[0]}
}

func sortedRows(descending bool, orderByStrings ...string) []*FlatRow {
	rows := buildRows()

	orderBy := make([]OrderBy, 0, len(orderByStrings))
	for _, field := range orderByStrings {
		orderBy = append(orderBy, NewOrderBy(field, descending))
	}
	sort.Sort(&orderedRows{
		orderBy: orderBy,
		rows:    rows,
	})

	return rows
}

func buildRows() []*FlatRow {
	strs := []string{"c", "a", "b", "c", "", "b"}
	bools := []bool{true, false, false, true, true, false}
	vals := []float64{56, 78, 23, 56, 12, 0}

	rows := make([]*FlatRow, 0, len(strs))
	for i, str := range strs {
		rows = append(rows, &FlatRow{
			TS:     int64(i),
			Key:    bytemap.New(map[string]interface{}{"s": str, "b": bools[i]}),
			Values: []float64{vals[i]},
			fields: []Field{NewField("val", expr.FIELD("val"))},
		})
	}

	return rows
}
