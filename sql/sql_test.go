package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/goexpr/geo"
	"github.com/getlantern/goexpr/isp"
	"github.com/getlantern/zenodb/core"
	. "github.com/getlantern/zenodb/expr"
	"github.com/kylelemons/godebug/pretty"
	"github.com/stretchr/testify/assert"
)

func TestSQL(t *testing.T) {
	RegisterUnaryDIMFunction("TEST", func(val goexpr.Expr) goexpr.Expr {
		return &testexpr{val}
	})
	known := AVG("k")
	knownField := core.NewField("knownfield", known)
	oKnownField := core.NewField("oknownfield", SUM("o"))
	xKnownField := core.NewField("x", SUM("x"))
	q, err := Parse(`
SELECT
	AVG(a) / (SUM(A) + SUM(b) + SUM(C)) * 2 AS rate,
	myfield,
	knownfield,
	IF(dim = 'test', AVG(myfield)) AS the_avg,
	*,
	SUM(BOUNDED(bfield, 0, 100)) AS bounded
FROM Table_A ASOF '-60m' UNTIL '-15m'
WHERE
	Dim_a LIKE '172.56.' AND
	dim_b > 10 OR (dim_c = 20 OR dim_d <> 'thing') AND
	dim_e NOT LIKE 'no such host' AND
	dim_f != true AND
	dim_g IS NULL AND
	dim_h IS NOT NULL AND
	dim_i IN (5, 6, 7, 8) AND
	dim_j IN (SELECT subdim FROM subtable WHERE subdim > 20) AND
	RAND() < 0.5
GROUP BY
	dim_a,
	CROSSTAB(dim_b),
	ISP(ip) AS isp,
	ORG(ip) AS org,
	ASN(ip) AS asn,
	CITY(ip) AS city,
	REGION(ip) AS state,
	REGION_CITY(ip) AS city_state,
	COUNTRY_CODE(ip) AS country,
	CONCAT('|', part_a, part_b) AS joined,
	TEST(dim_k) AS test_dim_k,
	period('5s') // period is a special function
HAVING Rate > 15 AND H < 2
ORDER BY Rate DESC, x, y
LIMIT 100, 10
`, func(table string) ([]core.Field, error) {
		if table == "table_a" {
			return []core.Field{knownField, oKnownField, xKnownField}, nil
		}
		if table == "subtable" {
			return []core.Field{}, nil
		}
		return nil, fmt.Errorf("Unknown table %v", table)
	})
	if !assert.NoError(t, err) {
		return
	}
	rate := MULT(DIV(AVG("a"), ADD(ADD(SUM("a"), SUM("b")), SUM("c"))), 2)
	myfield := SUM("myfield")
	if assert.Len(t, q.Fields, 7) {
		field := q.Fields[0]
		expected := core.NewField("rate", rate).String()
		actual := field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[1]
		expected = core.NewField("myfield", myfield).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[2]
		expected = knownField.String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[3]
		cond, err := goexpr.Binary("==", goexpr.Param("dim"), goexpr.Constant("test"))
		if !assert.NoError(t, err) {
			return
		}
		ifEx, err := IF(cond, AVG("myfield"))
		if !assert.NoError(t, err) {
			return
		}
		expected = core.NewField("the_avg", ifEx).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[4]
		expected = oKnownField.String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[5]
		expected = xKnownField.String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[6]
		expected = core.NewField("bounded", SUM(BOUNDED("bfield", 0, 100))).String()
		actual = field.String()
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, "table_a", q.From)
	if assert.Len(t, q.GroupBy, 10) {
		assert.Equal(t, core.NewGroupBy("asn", isp.ASN(goexpr.Param("ip"))).String(), q.GroupBy[0].String())
		assert.Equal(t, core.NewGroupBy("city", geo.CITY(goexpr.Param("ip"))), q.GroupBy[1])
		assert.Equal(t, core.NewGroupBy("city_state", geo.REGION_CITY(goexpr.Param("ip"))), q.GroupBy[2])
		assert.Equal(t, core.NewGroupBy("country", geo.COUNTRY_CODE(goexpr.Param("ip"))), q.GroupBy[3])
		assert.Equal(t, core.NewGroupBy("dim_a", goexpr.Param("dim_a")), q.GroupBy[4])
		assert.Equal(t, core.NewGroupBy("isp", isp.ISP(goexpr.Param("ip"))).String(), q.GroupBy[5].String())
		assert.Equal(t, core.NewGroupBy("joined", goexpr.Concat(goexpr.Constant("|"), goexpr.Param("part_a"), goexpr.Param("part_b"))), q.GroupBy[6])
		assert.Equal(t, core.NewGroupBy("org", isp.ORG(goexpr.Param("ip"))).String(), q.GroupBy[7].String())
		assert.Equal(t, core.NewGroupBy("state", geo.REGION(goexpr.Param("ip"))), q.GroupBy[8])
		assert.Equal(t, core.NewGroupBy("test_dim_k", &testexpr{goexpr.Param("dim_k")}), q.GroupBy[9])
	}
	assert.False(t, q.GroupByAll)
	assert.Equal(t, goexpr.Param("dim_b"), q.Crosstab)
	assert.Equal(t, -60*time.Minute, q.AsOfOffset)
	assert.Equal(t, -15*time.Minute, q.UntilOffset)
	if assert.Len(t, q.OrderBy, 3) {
		assert.Equal(t, "rate", q.OrderBy[0].Field)
		assert.True(t, q.OrderBy[0].Descending)
		assert.Equal(t, "x", q.OrderBy[1].Field)
		assert.False(t, q.OrderBy[1].Descending)
		assert.Equal(t, "y", q.OrderBy[2].Field)
		assert.False(t, q.OrderBy[2].Descending)
	}
	assert.Equal(t, 5*time.Second, q.Resolution)
	// TODO: reenable this
	// assert.Equal(t, "(((dim_a LIKE 172.56.) AND (dim_b > 10)) OR (((((((dim_c == 20) OR (dim_d != thing)) AND (dim_e LIKE no such host)) AND (dim_f != true)) AND (dim_g == <nil>)) AND (dim_h != <nil>)) AND dim_i IN(5, 6, 7, 8)))", q.Where.String())
	assert.Equal(t, "where dim_a like '172.56.' and dim_b > 10 or (dim_c = 20 or dim_d != 'thing') and dim_e not like 'no such host' and dim_f != true and dim_g is null and dim_h is not null and dim_i in (5, 6, 7, 8) and dim_j in (select subdim as subdim from subtable where subdim > 20) and rand() < 0.5", q.WhereSQL)
	expectedHaving := AND(GT(rate, 15), LT(SUM("h"), 2)).String()
	actualHaving := q.Having.String()
	assert.Equal(t, expectedHaving, actualHaving)
	assert.Equal(t, 10, q.Limit)
	assert.Equal(t, 100, q.Offset)
	assert.EqualValues(t, []string{"_points", "a", "b", "bfield", "c", "h", "knownfield", "myfield", "oknownfield", "rate", "x"}, q.IncludedFields)
	assert.EqualValues(t, []string{"dim", "dim_a", "dim_b", "dim_c", "dim_d", "dim_e", "dim_f", "dim_g", "dim_h", "dim_i", "dim_j", "dim_k", "ip", "part_a", "part_b"}, q.IncludedDims)
}

func TestFromSubQuery(t *testing.T) {
	field := core.NewField("field", MAX("field"))
	fieldSource := func(table string) ([]core.Field, error) {
		if table != "the_table" {
			return nil, fmt.Errorf("Table %v not found", table)
		}
		return []core.Field{field}, nil
	}
	subSQL := "SELECT name, * FROM the_table ASOF '-2h' UNTIL '-1h' GROUP BY *, period('5s') HAVING stuff > 5"
	subQuery, err := Parse(subSQL, fieldSource)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, -2*time.Hour, subQuery.AsOfOffset)
	assert.Equal(t, -1*time.Hour, subQuery.UntilOffset)
	q, err := Parse(fmt.Sprintf(`
SELECT AVG(field) AS the_avg, *
FROM (%s)
GROUP BY *, period('10s')
`, subSQL), fieldSource)
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, q.From)
	if !assert.NotNil(t, q.FromSubQuery) {
		return
	}
	assert.Equal(t, -2*time.Hour, q.AsOfOffset)
	assert.Equal(t, -1*time.Hour, q.UntilOffset)
	assert.Empty(t, pretty.Compare(q.FromSubQuery, subQuery))
	if assert.Len(t, q.Fields, 3) {
		field := q.Fields[0]
		expected := core.NewField("the_avg", AVG("field")).String()
		actual := field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[1]
		expected = core.NewField("name", SUM("name")).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = q.Fields[2]
		expected = core.NewField("field", MAX("field")).String()
		actual = field.String()
		assert.Equal(t, expected, actual)
	}
}

func TestSQLDefaults(t *testing.T) {
	q, err := Parse(`
SELECT _
FROM Table_A
`, func(table string) ([]core.Field, error) {
		return []core.Field{}, nil
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, q.Fields)
	assert.True(t, q.GroupByAll)
}

type testexpr struct {
	val goexpr.Expr
}

func (e *testexpr) Eval(params goexpr.Params) interface{} {
	v := e.val.Eval(params)
	return fmt.Sprintf("test: %v", v)
}

func (e *testexpr) String() string {
	return fmt.Sprintf("TEST(%v)", e.val.String())
}
