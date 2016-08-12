package zenodb

import (
	"sort"
	"testing"

	"github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/sql"
	"github.com/stretchr/testify/assert"
)

func TestSortTime(t *testing.T) {
	rows := sortedRows(false, "_time")
	assert.Equal(t, []int{5, 4, 3, 2, 1, 0}, actualTimes(rows))

	rows = sortedRows(true, "_time")
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, actualTimes(rows))
}

func TestSortBools(t *testing.T) {
	rows := sortedRows(false, "bool")
	assert.Equal(t, []bool{false, false, false, true, true, true}, actualBools(rows))

	rows = sortedRows(true, "bool")
	assert.Equal(t, []bool{true, true, true, false, false, false}, actualBools(rows))
}

func TestSortString(t *testing.T) {
	rows := sortedRows(false, "string")
	assert.Equal(t, []string{"", "a", "b", "b", "c", "c"}, actualStrings(rows))

	rows = sortedRows(true, "string")
	assert.Equal(t, []string{"c", "c", "b", "b", "a", ""}, actualStrings(rows))
}

func TestSortVals(t *testing.T) {
	rows := sortedRows(false, "val")
	assert.Equal(t, []float64{0, 12, 23, 56, 56, 78}, actualVals(rows))

	rows = sortedRows(true, "val")
	assert.Equal(t, []float64{78, 56, 56, 23, 12, 0}, actualVals(rows))
}

func TestSortAll(t *testing.T) {
	rows := sortedRows(false, "bool", "string", "val", "_time")
	assert.Equal(t, []bool{false, false, false, true, true, true}, actualBools(rows))
	assert.Equal(t, []string{"a", "b", "b", "", "c", "c"}, actualStrings(rows))
	assert.Equal(t, []float64{78, 0, 23, 12, 56, 56}, actualVals(rows))
	assert.Equal(t, []int{1, 5, 2, 4, 3, 0}, actualTimes(rows))

	rows = sortedRows(true, "bool", "string", "val", "_time")
	assert.Equal(t, []bool{true, true, true, false, false, false}, actualBools(rows))
	assert.Equal(t, []string{"c", "c", "", "b", "b", "a"}, actualStrings(rows))
	assert.Equal(t, []float64{56, 56, 12, 23, 0, 78}, actualVals(rows))
	assert.Equal(t, []int{0, 3, 4, 2, 5, 1}, actualTimes(rows))
}

func actualTimes(rows []*Row) []int {
	return []int{rows[0].Period, rows[1].Period, rows[2].Period, rows[3].Period, rows[4].Period, rows[5].Period}
}

func actualStrings(rows []*Row) []string {
	return []string{rows[0].Dims[0].(string), rows[1].Dims[0].(string), rows[2].Dims[0].(string), rows[3].Dims[0].(string), rows[4].Dims[0].(string), rows[5].Dims[0].(string)}
}

func actualBools(rows []*Row) []bool {
	return []bool{rows[0].Dims[1].(bool), rows[1].Dims[1].(bool), rows[2].Dims[1].(bool), rows[3].Dims[1].(bool), rows[4].Dims[1].(bool), rows[5].Dims[1].(bool)}
}

func actualVals(rows []*Row) []float64 {
	return []float64{rows[0].Values[0], rows[1].Values[0], rows[2].Values[0], rows[3].Values[0], rows[4].Values[0], rows[5].Values[0]}
}

func sortedRows(descending bool, orderByStrings ...string) []*Row {
	rows := buildRows()

	orderBy := make([]sql.Order, 0, len(orderByStrings))
	for _, field := range orderByStrings {
		orderBy = append(orderBy, sql.Order{Field: field, Descending: descending})
	}
	sort.Sort(&orderedRows{
		orderBy: orderBy,
		rows:    rows,
	})

	return rows
}

func buildRows() []*Row {
	strs := []string{"c", "a", "b", "c", "", "b"}
	bools := []bool{true, false, false, true, true, false}
	vals := []float64{56, 78, 23, 56, 12, 0}

	groupBy := []string{"string", "bool"}
	fields := []sql.Field{
		sql.Field{
			Expr: expr.SUM("val"),
			Name: "val",
		},
	}

	rows := make([]*Row, 0, len(strs))
	for i, str := range strs {
		rows = append(rows, &Row{
			Period:  i,
			Dims:    []interface{}{str, bools[i]},
			Values:  []float64{vals[i]},
			groupBy: groupBy,
			fields:  fields,
		})
	}

	return rows
}
