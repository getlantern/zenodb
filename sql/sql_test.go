package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/goexpr/geo"
	"github.com/getlantern/goexpr/isp"
	"github.com/getlantern/goexpr/redis"
	"github.com/getlantern/zenodb/core"
	. "github.com/getlantern/zenodb/expr"
	"github.com/kylelemons/godebug/pretty"
	"github.com/stretchr/testify/assert"
)

func TestParseDuration(t *testing.T) {
	doParse := func(s string) time.Duration {
		d, _ := ParseDuration(s)
		return d
	}
	assert.Equal(t, week+2*day+3*time.Hour+4*time.Minute+5*time.Second, doParse("1w2d3h4m5s"))
}

func TestDurationToString(t *testing.T) {
	assert.Equal(t, "23h55m0s", durationToString(23*time.Hour+55*time.Minute))
	assert.Equal(t, "1d23h55m0s", durationToString(day+23*time.Hour+55*time.Minute))
	assert.Equal(t, "1w1d23h55m0s", durationToString(week+day+23*time.Hour+55*time.Minute))
	assert.Equal(t, "1w0d23h55m0s", durationToString(week+23*time.Hour+55*time.Minute))
	assert.Equal(t, "1w", durationToString(week))
	assert.Equal(t, "1d", durationToString(day))
}

func TestSQLPlain(t *testing.T) {
	RegisterUnaryDIMFunction("TEST", func(val goexpr.Expr) goexpr.Expr {
		return &testexpr{val}
	})
	RegisterAlias("MYALIAS", "ANY(%v, PHGET('hash', %v), %v)")
	known := AVG("k")
	knownField := core.NewField("knownfield", known)
	oKnownField := core.NewField("oknownfield", SUM("o"))
	xKnownField := core.NewField("x", SUM("x"))
	q, err := Parse(`
SELECT
	AVG(a) / (SUM(A) + SUM(b) + SUM(C)) * 2 AS rate,
	myfield,
	` + "`knownfield`" + `,
	IF(dim = 'test', AVG(myfield)) AS the_avg,
	*,
	SUM(BOUNDED(bfield, 0, 100)) AS bounded,
	5 as cval,
	WAVG(a, b) AS weighted,
	IF(dim = 'test2', _) AS present,
	SHIFT(SUM(s), '1h') AS shifted,
	CROSSHIFT(cs, '-1w', '1d')
FROM Table_A ASOF '-1w' UNTIL '-15m'
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
	CROSSTAB(dim_b, dim_ct),
	PISP(ip) AS isp,
	ORG(ip) AS org,
	ASN(ip) AS asn,
	ASNAME(ip) AS asn_name,
	CITY(ip) AS city,
	REGION(ip) AS state,
	REGION_CITY(ip) AS city_state,
	COUNTRY_CODE(ip) AS country,
	PCONCAT('|', part_a, part_b) AS joined,
	TEST(dim_k) AS test_dim_k,
	MyAlias(dim_l, dim_m, dim_n) AS any_of_three,
	SPLIT(dim_o, ',', 2) AS spl,
	PSUBSTR(dim_p, 1, 5) AS sub,
	LEN(dim_q) AS qlen,
	period('5s'), // period is a special function
	STRIDE('1d')
HAVING Rate > 15 AND H < 2
ORDER BY Rate DESC, x, y
LIMIT 100, 10
`)

	tableFields := core.Fields{knownField, oKnownField, xKnownField}
	if !assert.NoError(t, err) {
		return
	}
	rate := MULT(DIV(AVG("a"), ADD(ADD(SUM("a"), SUM("b")), SUM("c"))), 2)
	myfield := SUM("myfield")
	assert.Equal(t, "avg(a)/(sum(a)+sum(b)+sum(c))*2 as rate, myfield, knownfield, if(dim = 'test', avg(myfield)) as the_avg, *, sum(bounded(bfield, 0, 100)) as bounded, 5 as cval, wavg(a, b) as weighted, if(dim = 'test2', _) as present, shift(sum(s), '1h') as shifted, crosshift(cs, '-1w', '1d'), rate > 15 and h < 2 AS _having", q.Fields.String())
	fields, err := q.Fields.Get(tableFields)
	if !assert.NoError(t, err) {
		return
	}
	fieldsNoHaving, err := q.FieldsNoHaving.Get(tableFields)
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, fieldsNoHaving, 18)
	if assert.Len(t, fields, 19) {
		field := fields[0]
		expected := core.NewField("rate", rate).String()
		actual := field.String()
		assert.Equal(t, expected, actual)

		field = fields[1]
		expected = core.NewField("myfield", myfield).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[2]
		expected = knownField.String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[3]
		cond, err := goexpr.Binary("==", goexpr.Param("dim"), goexpr.Constant("test"))
		if !assert.NoError(t, err) {
			return
		}
		ifEx := IF(cond, AVG("myfield"))
		expected = core.NewField("the_avg", ifEx).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[4]
		expected = oKnownField.String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[5]
		expected = xKnownField.String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[6]
		expected = core.NewField("bounded", SUM(BOUNDED("bfield", 0, 100))).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[7]
		expected = core.NewField("cval", CONST(5)).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[8]
		expected = core.NewField("weighted", WAVG("a", "b")).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[9]
		cond, err = goexpr.Binary("==", goexpr.Param("dim"), goexpr.Constant("test2"))
		if !assert.NoError(t, err) {
			return
		}
		ifEx = IF(cond, GT(PointsField.Expr, CONST(0)))
		expected = core.NewField("present", ifEx).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[10]
		expected = core.NewField("shifted", SHIFT(SUM("s"), 1*time.Hour)).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		for i := time.Duration(0); i < 7; i++ {
			field = fields[11+i]
			as := "cs"
			if i > 0 {
				as = fmt.Sprintf("cs_%dd", i)
			}
			expected = core.NewField(as, SHIFT(SUM("cs"), i*-24*time.Hour)).String()
			actual = field.String()
			assert.Equal(t, expected, actual)
		}

		field = fields[18]
		expected = core.NewField("_having", AND(GT(rate, 15), LT(SUM("h"), 2))).String()
		actual = field.String()
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, "table_a", q.From)
	assert.Equal(t, "Table_A", q.FromSQL)
	if assert.Len(t, q.GroupBy, 15) {
		idx := 0
		assert.Equal(t, core.NewGroupBy("any_of_three", goexpr.Any(goexpr.Param("dim_l"), goexpr.P(redis.HGet(goexpr.Constant("hash"), goexpr.Param("dim_m"))), goexpr.Param("dim_n"))).String(), q.GroupBy[idx].String())
		idx++
		assert.Equal(t, core.NewGroupBy("asn", isp.ASN(goexpr.Param("ip"))).String(), q.GroupBy[idx].String())
		idx++
		assert.Equal(t, core.NewGroupBy("asn_name", isp.ASName(goexpr.Param("ip"))).String(), q.GroupBy[idx].String())
		idx++
		assert.Equal(t, core.NewGroupBy("city", geo.CITY(goexpr.Param("ip"))), q.GroupBy[idx])
		idx++
		assert.Equal(t, core.NewGroupBy("city_state", geo.REGION_CITY(goexpr.Param("ip"))), q.GroupBy[idx])
		idx++
		assert.Equal(t, core.NewGroupBy("country", geo.COUNTRY_CODE(goexpr.Param("ip"))), q.GroupBy[idx])
		idx++
		assert.Equal(t, core.NewGroupBy("dim_a", goexpr.Param("dim_a")), q.GroupBy[idx])
		idx++
		assert.Equal(t, core.NewGroupBy("isp", goexpr.P(isp.ISP(goexpr.Param("ip")))).String(), q.GroupBy[idx].String())
		idx++
		assert.Equal(t, core.NewGroupBy("joined", goexpr.P(goexpr.Concat(goexpr.Constant("|"), goexpr.Param("part_a"), goexpr.Param("part_b")))).String(), q.GroupBy[idx].String())
		idx++
		assert.Equal(t, core.NewGroupBy("org", isp.ORG(goexpr.Param("ip"))).String(), q.GroupBy[idx].String())
		idx++
		assert.Equal(t, core.NewGroupBy("qlen", goexpr.Len(goexpr.Param("dim_q"))).String(), q.GroupBy[idx].String())
		idx++
		assert.Equal(t, core.NewGroupBy("spl", goexpr.Split(goexpr.Param("dim_o"), goexpr.Constant(","), goexpr.Constant(2))).String(), q.GroupBy[idx].String())
		assert.Equal(t, "c", q.GroupBy[idx].Expr.Eval(goexpr.MapParams{"dim_o": "a,b,c"}))
		idx++
		assert.Equal(t, core.NewGroupBy("state", geo.REGION(goexpr.Param("ip"))), q.GroupBy[idx])
		idx++
		assert.Equal(t, core.NewGroupBy("sub", goexpr.P(goexpr.Substr(goexpr.Param("dim_p"), goexpr.Constant(1), goexpr.Constant(5)))).String(), q.GroupBy[idx].String())
		assert.Equal(t, "bcdef", q.GroupBy[idx].Expr.Eval(goexpr.MapParams{"dim_p": "abcdefg"}))
		idx++
		assert.Equal(t, core.NewGroupBy("test_dim_k", &testexpr{goexpr.Param("dim_k")}), q.GroupBy[idx])
	}
	assert.False(t, q.GroupByAll)
	assert.Equal(t, goexpr.Concat(goexpr.Constant("_"), goexpr.Param("dim_b"), goexpr.Param("dim_ct")), q.Crosstab)
	assert.Equal(t, -7*24*time.Hour, q.AsOfOffset)
	assert.Equal(t, -15*time.Minute, q.UntilOffset)
	assert.Equal(t, 24*time.Hour, q.Stride)
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
	assert.Equal(t, "where dim_a like '172.56.' and dim_b > 10 or (dim_c = 20 or dim_d != 'thing') and dim_e not like 'no such host' and dim_f != true and dim_g is null and dim_h is not null and dim_i in (5, 6, 7, 8) and dim_j in (select subdim from subtable where subdim > 20) and rand() < 0.5", q.WhereSQL)
	var subQueries []*SubQuery
	q.Where.WalkLists(func(list goexpr.List) {
		sq, ok := list.(*SubQuery)
		if ok {
			subQueries = append(subQueries, sq)
		}
	})
	if assert.Len(t, subQueries, 1) {
		assert.Equal(t, "select subdim from subtable where subdim > 20", subQueries[0].SQL)
	}
	assert.True(t, q.HasHaving)
	assert.Equal(t, "rate > 15 and h < 2 AS _having", q.HavingSQL)
	assert.Equal(t, 10, q.Limit)
	assert.Equal(t, 100, q.Offset)
}

func TestFromSubQuery(t *testing.T) {
	subSQL := "SELECT name, * FROM the_table ASOF '-2h' UNTIL '-1h' GROUP BY CONCAT(',', A, B) AS A, period('5s') HAVING stuff > 5"
	subQuery, err := Parse(subSQL)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, -2*time.Hour, subQuery.AsOfOffset)
	assert.Equal(t, -1*time.Hour, subQuery.UntilOffset)
	q, err := Parse(fmt.Sprintf(`
SELECT AVG(field) AS the_avg, name, MAX(field) AS field
FROM (%s)
GROUP BY A, period('10s')
`, subSQL))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "(select name, * from the_table ASOF '-2h' UNTIL '-1h' group by concat(',', a, b) as a, period('5s') having stuff > 5)", q.FromSQL)
	assert.Empty(t, q.From)
	if !assert.NotNil(t, q.FromSubQuery) {
		return
	}
	assert.Equal(t, -2*time.Hour, q.AsOfOffset)
	assert.Equal(t, -1*time.Hour, q.UntilOffset)
	assert.Empty(t, pretty.Compare(q.FromSubQuery, subQuery))
	assert.Equal(t, "avg(field) as the_avg, name, max(field) as field", q.Fields.String())
	fields, err := q.Fields.Get(nil)
	if !assert.NoError(t, err) {
		return
	}
	if assert.Len(t, fields, 3) {
		field := fields[0]
		expected := core.NewField("the_avg", AVG("field")).String()
		actual := field.String()
		assert.Equal(t, expected, actual)

		field = fields[1]
		expected = core.NewField("name", SUM("name")).String()
		actual = field.String()
		assert.Equal(t, expected, actual)

		field = fields[2]
		expected = core.NewField("field", MAX("field")).String()
		actual = field.String()
		assert.Equal(t, expected, actual)
	}

	if assert.Len(t, q.GroupBy, 1) {
		assert.Equal(t, core.NewGroupBy("a", goexpr.Param("a")), q.GroupBy[0])
	}
}

func TestSQLDefaults(t *testing.T) {
	q, err := Parse(`
SELECT _
FROM Table_A
`)
	if !assert.NoError(t, err) {
		return
	}
	fields, err := q.Fields.Get(nil)
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, fields)
	assert.True(t, q.GroupByAll)
}

func TestParseIt(t *testing.T) {
	_, err := Parse(`select * from TableA  group by concat('_', ct1, concat('|', ct2)) as _crosstab`)
	assert.NoError(t, err)
}

type testexpr struct {
	val goexpr.Expr
}

func (e *testexpr) Eval(params goexpr.Params) interface{} {
	v := e.val.Eval(params)
	return fmt.Sprintf("test: %v", v)
}

func (e *testexpr) WalkParams(cb func(string)) {
	e.val.WalkParams(cb)
}

func (e *testexpr) WalkOneToOneParams(cb func(string)) {
	e.val.WalkOneToOneParams(cb)
}

func (e *testexpr) WalkLists(cb func(goexpr.List)) {
	e.val.WalkLists(cb)
}

func (e *testexpr) String() string {
	return fmt.Sprintf("TEST(%v)", e.val.String())
}
