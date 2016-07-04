package tdb

import (
	"testing"
	"time"

	. "github.com/oxtoacart/tdb/expr"
	"github.com/stretchr/testify/assert"
)

func TestSQL(t *testing.T) {
	aq := &Query{}
	err := aq.applySQL(`
SELECT AVG(a / (A + b + C)) * 2 AS rate
FROM Table_A ASOF '-60m' UNTIL '-15m'
WHERE Dim_a LIKE '172.56.' AND (dim_b > 10 OR dim_c = 20) OR dim_d != 'thing'
GROUP BY dim_A, period('5s') // period is a special function
HAVING AVG(Rate) > 15
ORDER BY AVG(Rate) DESC
LIMIT 100, 10
`)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, aq.fields, 1) {
		expected := MULT(AVG(DIV("a", ADD(ADD("a", "b"), "c"))), 2).String()
		actual := aq.fields["rate"].String()
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, "table_a", aq.table)
	if assert.Len(t, aq.dims, 1) {
		assert.Equal(t, "dim_a", aq.dims[0])
	}
	assert.Equal(t, -60*time.Minute, aq.fromOffset)
	assert.Equal(t, -15*time.Minute, aq.toOffset)
	if assert.Len(t, aq.orderBy, 1) {
		expected := MULT(-1, AVG("rate")).String()
		actual := aq.orderBy[0].String()
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, 5*time.Second, aq.resolution)
	assert.Equal(t, "dim_a =~ '172.56.' && (dim_b > 10 || dim_c == 20) || dim_d != 'thing'", aq.filter)
	expectedHaving := GT(AVG("rate"), 15).String()
	actualHaving := aq.having.String()
	assert.Equal(t, expectedHaving, actualHaving)
	assert.Equal(t, 10, aq.limit)
	assert.Equal(t, 100, aq.offset)
}
