package sql

import (
	"testing"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/goexpr/geo"
	"github.com/getlantern/goexpr/isp"
	. "github.com/getlantern/zenodb/expr"
	"github.com/stretchr/testify/assert"
)

func TestSQL(t *testing.T) {
	known := AVG("k")
	knownfield := Field{known, "knownfield"}
	q, err := Parse(`
SELECT
	AVG(a) / (SUM(A) + SUM(b) + SUM(C)) * 2 AS rate,
	myfield,
	knownfield,
	IF(dim = 'test', AVG(myfield)) AS the_avg
FROM Table_A ASOF '-60m' UNTIL '-15m'
WHERE Dim_a LIKE '172.56.' AND (dim_b > 10 OR dim_c = 20) OR dim_d <> 'thing' AND dim_e NOT LIKE 'no such host' AND dim_f != true
GROUP BY
	dim_a,
	CROSSTAB(dim_b),
	ISP(ip) AS isp,
	ASN(ip) AS asn,
	CITY(ip) AS city,
	REGION(ip) AS state,
	REGION_CITY(ip) AS city_state,
	COUNTRY_CODE(ip) AS country,
	CONCAT('|', part_a, part_b) AS joined,
	period('5s') // period is a special function
HAVING Rate > 15 AND H < 2
ORDER BY Rate DESC, X
LIMIT 100, 10
`, knownfield)
	if !assert.NoError(t, err) {
		return
	}
	rate := MULT(DIV(AVG("a"), ADD(ADD(SUM("a"), SUM("b")), SUM("c"))), 2)
	myfield := SUM("myfield")
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
		cond, err := goexpr.Binary("==", goexpr.Param("dimension"), goexpr.Constant("test"))
		if !assert.NoError(t, err) {
			return
		}
		ifEx, err := IF(cond, AVG("myfield"))
		if !assert.NoError(t, err) {
			return
		}
		expected = Field{ifEx, "the_avg"}.String()
		actual = field.String()
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, "table_a", q.From)
	if assert.Len(t, q.GroupBy, 8) {
		assert.Equal(t, NewGroupBy("asn", isp.ASN(goexpr.Param("ip"))), q.GroupBy[0])
		assert.Equal(t, NewGroupBy("city", geo.CITY(goexpr.Param("ip"))), q.GroupBy[1])
		assert.Equal(t, NewGroupBy("city_state", geo.REGION_CITY(goexpr.Param("ip"))), q.GroupBy[2])
		assert.Equal(t, NewGroupBy("country", geo.COUNTRY_CODE(goexpr.Param("ip"))), q.GroupBy[3])
		assert.Equal(t, NewGroupBy("dim_a", goexpr.Param("dim_a")), q.GroupBy[4])
		assert.Equal(t, NewGroupBy("isp", isp.ISP(goexpr.Param("ip"))), q.GroupBy[5])
		assert.Equal(t, NewGroupBy("joined", goexpr.Concat(goexpr.Constant("|"), goexpr.Param("part_a"), goexpr.Param("part_b"))), q.GroupBy[6])
		assert.Equal(t, NewGroupBy("state", geo.REGION(goexpr.Param("ip"))), q.GroupBy[7])
	}
	assert.False(t, q.GroupByAll)
	assert.Equal(t, goexpr.Param("dim_b"), q.Crosstab)
	assert.Equal(t, -60*time.Minute, q.AsOfOffset)
	assert.Equal(t, -15*time.Minute, q.UntilOffset)
	if assert.Len(t, q.OrderBy, 2) {
		assert.Equal(t, "rate", q.OrderBy[0].Field)
		assert.True(t, q.OrderBy[0].Descending)
		assert.Equal(t, "x", q.OrderBy[1].Field)
		assert.False(t, q.OrderBy[1].Descending)
	}
	assert.Equal(t, 5*time.Second, q.Resolution)
	// TODO: reenable this
	log.Debug(q.Where)
	// assert.Equal(t, "dim_a =~ '172.56.' && (dim_b > 10 || dim_c == 20) || dim_d != 'thing' && dim_e !~ 'no such host'", q.Where.String())
	expectedHaving := AND(GT(rate, 15), LT(SUM("h"), 2)).String()
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
