package sql

import (
	"testing"
	"time"

	. "github.com/getlantern/tdb/expr"
	"github.com/stretchr/testify/assert"
)

func TestSQL(t *testing.T) {
	q, err := Parse(`
SELECT AVG(a / (A + b + C)) * 2 AS rate
FROM Table_A ASOF '-60m' UNTIL '-15m'
WHERE Dim_a LIKE '172.56.' AND (dim_b > 10 OR dim_c = 20) OR dim_d <> 'thing' AND dim_e NOT LIKE 'no such host'
GROUP BY dim_A, period('5s') // period is a special function
HAVING AVG(Rate) > 15
ORDER BY AVG(Rate) DESC
LIMIT 100, 10
`)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, q.Fields, 1) {
		field := q.Fields[0]
		expected := MULT(AVG(DIV("a", ADD(ADD("a", "b"), "c"))), 2).String()
		actual := field.String()
		assert.Equal(t, expected, actual)
		assert.Equal(t, "rate", field.Name)
	}
	assert.Equal(t, "table_a", q.From)
	if assert.Len(t, q.GroupBy, 1) {
		assert.Equal(t, "dim_a", q.GroupBy[0])
	}
	assert.Equal(t, -60*time.Minute, q.AsOfOffset)
	assert.Equal(t, -15*time.Minute, q.UntilOffset)
	if assert.Len(t, q.OrderBy, 1) {
		expected := MULT(-1, AVG("rate")).String()
		actual := q.OrderBy[0].String()
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, 5*time.Second, q.Resolution)
	assert.Equal(t, "dim_a =~ '172.56.' && (dim_b > 10 || dim_c == 20) || dim_d != 'thing' && dim_e !~ 'no such host'", q.Where)
	expectedHaving := GT(AVG("rate"), 15).String()
	actualHaving := q.Having.String()
	assert.Equal(t, expectedHaving, actualHaving)
	assert.Equal(t, 10, q.Limit)
	assert.Equal(t, 100, q.Offset)
}
