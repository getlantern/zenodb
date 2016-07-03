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
FROM Table_A
WHERE Dim_a LIKE '172.56.' AND (dim_b > 10 OR dim_c = 20) OR dim_d != 'thing'
GROUP BY dim_A, period('5s') // period is a special function
ORDER BY AVG(Rate) DESC
LIMIT '60m', '15m' // offsets are relative to current time (going backwards)
`)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, aq.fields, 1) {
		expected := ToString(MULT(AVG(DIV("a", ADD(ADD("a", "b"), "c"))), 2))
		actual := ToString(aq.fields["rate"])
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, "table_a", aq.table)
	if assert.Len(t, aq.dims, 1) {
		assert.Equal(t, "dim_a", aq.dims[0])
	}
	if assert.Len(t, aq.orderBy, 1) {
		expected := ToString(MULT(-1, AVG("rate")))
		actual := ToString(aq.orderBy[0])
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, 5*time.Second, aq.resolution)
	assert.Equal(t, "dim_a =~ '172.56.' && (dim_b > 10 || dim_c == 20) || dim_d != 'thing'", aq.filter)
	assert.Equal(t, 15*time.Minute, aq.limit)
	assert.Equal(t, 60*time.Minute, aq.offset)
}
