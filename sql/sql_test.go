package sql

import (
	"testing"
	"time"

	"github.com/Knetic/govaluate"
	. "github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
)

func TestSQL(t *testing.T) {
	known := AVG("k", nil)
	knownfield := Field{known, "knownfield"}
	q, err := Parse(`
SELECT
	AVG(a) / (SUM(A) + SUM(b) + SUM(C)) * 2 AS rate,
	myfield,
	knownfield,
	AVG(myfield, dimension = 'test') AS the_avg
FROM Table_A ASOF '-60m' UNTIL '-15m'
WHERE Dim_a LIKE '172.56.' AND (dim_b > 10 OR dim_c = 20) OR dim_d <> 'thing' AND dim_e NOT LIKE 'no such host'
GROUP BY dim_a, period('5s') // period is a special function
HAVING Rate > 15 AND H < 2
ORDER BY Rate DESC, X
LIMIT 100, 10
`, knownfield)
	if !assert.NoError(t, err) {
		return
	}
	rate := MULT(DIV(AVG("a", nil), ADD(ADD(SUM("a", nil), SUM("b", nil)), SUM("c", nil))), 2)
	myfield := SUM("myfield", nil)
	if assert.Len(t, q.Fields, 4) {
		field := q.Fields[0]
		expected := Field{rate, "rate"}.String()
		actual := field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[1]
		expected = Field{myfield, "myfield"}.String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[2]
		expected = knownfield.String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[3]
		cond, _ := govaluate.NewEvaluableExpression("dimension == 'test'")
		expected = Field{AVG("myfield", cond), "the_avg"}.String()
		actual = field.String()
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, "table_a", q.From)
	if assert.Len(t, q.GroupBy, 1) {
		assert.Equal(t, "dim_a", q.GroupBy[0])
	}
	assert.False(t, q.GroupByAll)
	assert.Equal(t, -60*time.Minute, q.AsOfOffset)
	assert.Equal(t, -15*time.Minute, q.UntilOffset)
	if assert.Len(t, q.OrderBy, 2) {
		assert.Equal(t, "rate", q.OrderBy[0].Field)
		assert.True(t, q.OrderBy[0].Descending)
		assert.Equal(t, "x", q.OrderBy[1].Field)
		assert.False(t, q.OrderBy[1].Descending)
	}
	assert.Equal(t, 5*time.Second, q.Resolution)
	assert.Equal(t, "dim_a =~ '172.56.' && (dim_b > 10 || dim_c == 20) || dim_d != 'thing' && dim_e !~ 'no such host'", q.Where.String())
	expectedHaving := AND(GT(rate, 15), LT(SUM("h", nil), 2)).String()
	actualHaving := q.Having.String()
	assert.Equal(t, expectedHaving, actualHaving)
	assert.Equal(t, 10, q.Limit)
	assert.Equal(t, 100, q.Offset)
}

func TestSQLDefaults(t *testing.T) {
	q, err := Parse(`
SELECT SUM(a) AS the_sum
FROM Table_A
`)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, q.GroupByAll)
}
