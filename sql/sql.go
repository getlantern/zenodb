// Package sql provides the ability to parse SQL queries. This pacakge contains
// some code taking from the Go standard library's time package. Go is licensed
// as per https://golang.org/LICENSE.
package sql

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/goexpr/geo"
	"github.com/getlantern/goexpr/isp"
	"github.com/getlantern/goexpr/redis"
	"github.com/getlantern/golog"
	"github.com/getlantern/sqlparser"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/expr"
)

var (
	log = golog.LoggerFor("zenodb.sql")
)

var (
	ErrSelectNoName                  = errors.New("All expressions in SELECT must either reference a column name or include an AS alias")
	ErrIfArity                       = errors.New("IF requires two parameters, like IF(dim = 1, SUM(b))")
	ErrBoundedArity                  = errors.New("BOUNDED requires three parameters, like BOUNDED(b, 0, 100)")
	ErrPercentileArity               = errors.New("PERCENTILE requires either two or five parameters, like PERCENTILE(b, 99.9, 0, 1000, 3)")
	ErrPercentileOptWrap             = errors.New("PERCENTILE with two parameters may only wrap an existing PERCENTILE expression")
	ErrShiftArity                    = errors.New("SHIFT requires two parameters, like SHIFT(SUM(b), '-1h')")
	ErrCrosshiftArity                = errors.New("CROSSHIFT requires three parameters, like CROSSHIFT(SUM(b), '1h', '-1d')")
	ErrCrosshiftZeroCutoffOrInterval = errors.New("CROSSHIFT cutoff and interval must be non-zero")
	ErrCROSSTABArity                 = errors.New("CROSSTAB requires at least one argument")
	ErrCROSSTABUnique                = errors.New("Only one CROSSTAB statement allowed per query")
	ErrAggregateArity                = errors.New("Aggregate functions take only one parameter, like SUM(b)")
	ErrWildcardNotAllowed            = errors.New("Wildcard * is not supported")
	ErrNestedFunctionCall            = errors.New("Nested function calls are not currently supported in SELECT")
	ErrInvalidPeriod                 = errors.New("Please specify a period in the form period(5s) where 5s can be any valid Go duration expression")
	ErrInvalidStride                 = errors.New("Please specify a stride in the form stride(5s) where 5s can be any valid Go duration expression")
)

var aggregateFuncs = map[string]func(interface{}) expr.Expr{
	"SUM":   expr.SUM,
	"MIN":   expr.MIN,
	"MAX":   expr.MAX,
	"COUNT": expr.COUNT,
	"AVG":   expr.AVG,
}

var binaryAggregateFuncs = map[string]func(interface{}, interface{}) expr.Expr{
	"WAVG": expr.WAVG,
}

var operators = map[string]func(interface{}, interface{}) expr.Expr{
	"+": expr.ADD,
	"-": expr.SUB,
	"*": expr.MULT,
	"/": expr.DIV,
}

var conditions = map[string]func(interface{}, interface{}) expr.Expr{
	"<":  expr.LT,
	"<=": expr.LTE,
	"=":  expr.EQ,
	"<>": expr.NEQ,
	"!=": expr.NEQ,
	">=": expr.GTE,
	">":  expr.GT,
}

var nullaryGoExpr = map[string]func() goexpr.Expr{
	"RAND": goexpr.Rand,
}

var unaryGoExpr = map[string]func(goexpr.Expr) goexpr.Expr{
	"CITY":         geo.CITY,
	"REGION":       geo.REGION,
	"REGION_CITY":  geo.REGION_CITY,
	"COUNTRY_CODE": geo.COUNTRY_CODE,
	"ISP":          isp.ISP,
	"ORG":          isp.ORG,
	"ASN":          isp.ASN,
	"ASNAME":       isp.ASName,
	"LEN":          goexpr.Len,
}

var binaryGoExpr = map[string]func(goexpr.Expr, goexpr.Expr) goexpr.Expr{
	"HGET":      redis.HGet,
	"SISMEMBER": redis.SIsMember,
}

var ternaryGoExpr = map[string]func(goexpr.Expr, goexpr.Expr, goexpr.Expr) goexpr.Expr{
	"SPLIT":      goexpr.Split,
	"SUBSTR":     goexpr.Substr,
	"REPLACEALL": goexpr.ReplaceAll,
	"LUA": func(script goexpr.Expr, keys goexpr.Expr, args goexpr.Expr) goexpr.Expr {
		_keys := keys.(*goexpr.ArrayExpr)
		_args := args.(*goexpr.ArrayExpr)
		return redis.Lua(script, _keys.Items, _args.Items...)
	},
}

func crosstabExprFor(exprs ...goexpr.Expr) goexpr.Expr {
	// Crosstab expressions are concatenated using underscore
	allExprs := make([]goexpr.Expr, 1, len(exprs)+1)
	allExprs[0] = goexpr.Constant("_")
	allExprs = append(allExprs, exprs...)
	return goexpr.Concat(allExprs...)
}

var varGoExpr = map[string]func(...goexpr.Expr) goexpr.Expr{
	"CONCAT":    goexpr.Concat,
	"CROSSTAB":  crosstabExprFor,
	"CROSSTABT": crosstabExprFor,
	"ANY":       goexpr.Any,
	"ARRAY":     goexpr.Array,
}

func RegisterUnaryDIMFunction(name string, fn func(goexpr.Expr) goexpr.Expr) error {
	name = strings.ToUpper(name)
	_, found := unaryGoExpr[name]
	if found {
		return fmt.Errorf("Expression %v already registered", name)
	}
	unaryGoExpr[name] = fn
	return nil
}

var aliases = make(map[string]string)

func RegisterAlias(alias string, template string) {
	aliases[strings.ToUpper(alias)] = template
}

// SubQuery is a placeholder for a sub query within a query. Executors of a
// query should first execute all SubQueries and then call SetResult to set the
// results of the subquery. The subquery
type SubQuery struct {
	Dim    string
	SQL    string
	result []goexpr.Expr
}

func (sq *SubQuery) SetResult(result []interface{}) {
	sq.result = make([]goexpr.Expr, 0, len(result))
	for _, val := range result {
		sq.result = append(sq.result, goexpr.Constant(val))
	}
}

func (sq *SubQuery) Values() []goexpr.Expr {
	return sq.result
}

// Query represents the result of parsing a SELECT query.
type Query struct {
	SQL string
	// Fields are the fields from the SELECT clause in the order they appear,
	// including the synthetic _having field if there's a HAVING clause.
	Fields core.FieldSource
	// FieldsNoHaving is like Fields but without the synthetic HAVING clause
	FieldsNoHaving    core.FieldSource
	HasSelectAll      bool
	HasSpecificFields bool
	// From is the Table from the FROM clause
	From         string
	FromSubQuery *Query
	FromSQL      string
	Resolution   time.Duration
	Where        goexpr.Expr
	WhereSQL     string
	AsOf         time.Time
	AsOfOffset   time.Duration
	Until        time.Time
	UntilOffset  time.Duration
	Stride       time.Duration
	// GroupBy are the GroupBy expressions ordered alphabetically by name.
	GroupBy    []core.GroupBy
	GroupByAll bool
	// Crosstab is the goexpr.Expr used for crosstabs (goes into columns rather than rows)
	Crosstab              goexpr.Expr
	CrosstabIncludesTotal bool
	HasHaving             bool
	HavingSQL             string
	OrderBy               []core.OrderBy
	Offset                int
	Limit                 int
}

// TableFor returns the table in the FROM clause of this query
func TableFor(sql string) (string, error) {
	parsed, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}
	stmt := parsed.(*sqlparser.Select)
	return strings.ToLower(nodeToString(stmt.From[0])), nil
}

// Parse parses a SQL statement and returns a corresponding *Query object.
func Parse(sql string) (*Query, error) {
	parsed, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("Error parsing %v: %v", sql, err)
	}
	return parse(parsed.(*sqlparser.Select))
}

func parse(stmt *sqlparser.Select) (*Query, error) {
	q := &Query{
		SQL: nodeToString(stmt),
	}
	err := q.applyFrom(stmt)
	if err != nil {
		return nil, err
	}
	q.checkForFields(stmt)
	q.HasHaving = stmt.Having != nil
	if q.HasHaving {
		q.HavingSQL = fmt.Sprintf("%v AS %v", nodeToString(stmt.Having.Expr), core.HavingFieldName)
	}
	hasSelect := len(stmt.SelectExprs) > 0
	if hasSelect || q.HasHaving {
		var sql string
		if hasSelect && q.HasHaving {
			sql = fmt.Sprintf("%v, %v", nodeToString(stmt.SelectExprs), q.HavingSQL)
		} else if hasSelect {
			sql = nodeToString(stmt.SelectExprs)
		} else {
			sql = q.HavingSQL
		}
		combinedFields, combinedParseErr := sqlparser.Parse(fmt.Sprintf("SELECT %v FROM whatever", sql))
		if combinedParseErr != nil {
			return nil, fmt.Errorf("Unable to parse synthetic SQL query for combined fields: %v", combinedParseErr)
		}
		q.Fields = &selectClause{
			stmt:    combinedFields.(*sqlparser.Select),
			fielded: fielded{sql: sql},
		}
	}
	if hasSelect {
		q.FieldsNoHaving = &selectClause{
			stmt:    stmt,
			fielded: fielded{sql: nodeToString(stmt.SelectExprs)},
		}
	}
	if stmt.Where != nil {
		err = q.applyWhere(stmt)
		if err != nil {
			return nil, err
		}
	}
	if stmt.TimeRange != nil {
		err = q.applyTimeRange(stmt)
		if err != nil {
			return nil, err
		}
	}
	err = q.applyGroupBy(stmt)
	if err != nil {
		return nil, err
	}
	err = q.applyOrderBy(stmt)
	if err != nil {
		return nil, err
	}
	err = q.applyLimit(stmt)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Query) checkForFields(stmt *sqlparser.Select) {
	for _, _e := range stmt.SelectExprs {
		if nodeToString(_e) == "_" {
			// Ignore underscore
			continue
		}
		switch _e.(type) {
		case *sqlparser.StarExpr:
			q.HasSelectAll = true
		case *sqlparser.NonStarExpr:
			q.HasSpecificFields = true
		}
	}
}

type fielded struct {
	fieldsMap map[string]core.Field
	sql       string
}

func (f *fielded) init(known core.Fields) {
	f.fieldsMap = make(map[string]core.Field)
	for _, field := range known {
		f.fieldsMap[field.Name] = field
	}
}

func (f *fielded) String() string {
	return f.sql
}

type selectClause struct {
	stmt *sqlparser.Select
	fielded
}

func (s *selectClause) Get(known core.Fields) (core.Fields, error) {
	s.init(known)

	var fields core.Fields
	for _, _e := range s.stmt.SelectExprs {
		if nodeToString(_e) == "_" {
			// Ignore underscore
			continue
		}
		switch e := _e.(type) {
		case *sqlparser.StarExpr:
			for _, field := range known {
				fields = s.addField(fields, field)
			}
		case *sqlparser.NonStarExpr:
			var err error
			fe, ok := e.Expr.(*sqlparser.FuncExpr)
			if ok && strings.ToUpper(string(fe.Name)) == "CROSSHIFT" {
				// Special handling for CROSSHIFT
				fields, err = s.addCrosshiftExpr(fields, fe, e.As, true)
			} else {
				as, asErr := asOrColName(e.As, e.Expr)
				if asErr != nil {
					return nil, asErr
				}
				_fe, ferr := s.exprFor(e.Expr, true)
				if ferr != nil {
					return nil, ferr
				}
				fields, err = s.addExpr(fields, _fe, as)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	return fields, nil
}

func (s *selectClause) addCrosshiftExpr(fields core.Fields, e *sqlparser.FuncExpr, asBytes []byte, defaultToSum bool) (core.Fields, error) {
	if len(e.Exprs) != 3 {
		return nil, ErrCrosshiftArity
	}

	_valueEx, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	valueEx, valueErr := s.exprFor(_valueEx.Expr, true)
	if valueErr != nil {
		return nil, valueErr
	}
	as, asErr := asOrColName(asBytes, _valueEx.Expr)
	if asErr != nil {
		return nil, asErr
	}

	cutoff, cutoffErr := nodeToDuration(e.Exprs[1])
	if cutoffErr != nil {
		return nil, cutoffErr
	}
	if cutoff == 0 {
		return nil, ErrCrosshiftZeroCutoffOrInterval
	}

	interval, intervalErr := nodeToDuration(e.Exprs[2])
	if intervalErr != nil {
		return nil, intervalErr
	}
	if interval == 0 {
		return nil, ErrCrosshiftZeroCutoffOrInterval
	}
	if interval < 0 {
		interval = -1 * interval
	}

	limit := cutoff
	if cutoff < 0 {
		limit = cutoff * -1
	}

	var err error
	for i := time.Duration(0); i < limit; i += interval {
		newAs := as
		if i != 0 {
			newAs = fmt.Sprintf("%v_%v", as, durationToString(i))
		}
		shift := i
		if cutoff < 0 {
			shift = -1 * shift
		}
		fields, err = s.addExpr(fields, expr.SHIFT(valueEx, shift), newAs)
		if err != nil {
			return nil, err
		}
	}

	return fields, nil
}

func asOrColName(as []byte, e sqlparser.Expr) (string, error) {
	if len(as) > 0 {
		return string(as), nil
	}
	col, isColName := e.(*sqlparser.ColName)
	if !isColName {
		return "", ErrSelectNoName
	}
	return string(col.Name), nil
}

func (s *selectClause) addExpr(fields core.Fields, _fe interface{}, as string) (core.Fields, error) {
	fe, ok := _fe.(expr.Expr)
	if !ok {
		return nil, fmt.Errorf("Not an Expr: %v", _fe)
	}
	err := fe.Validate()
	if err != nil {
		return nil, fmt.Errorf("Invalid expression for '%s': %v", as, err)
	}
	fields = s.addField(fields, core.NewField(strings.ToLower(as), fe.(expr.Expr)))
	return fields, nil
}

func (s *selectClause) addField(fields core.Fields, field core.Field) core.Fields {
	fieldAlreadySelected := false
	for _, existingField := range fields {
		if existingField.Name == field.Name {
			fieldAlreadySelected = true
			break
		}
	}
	if !fieldAlreadySelected {
		fields = append(fields, field)
		s.fieldsMap[field.Name] = field
	}
	return fields
}

func (q *Query) applyFrom(stmt *sqlparser.Select) error {
	q.FromSQL = nodeToString(stmt.From)
	switch f := stmt.From[0].(type) {
	case *sqlparser.AliasedTableExpr:
		switch e := f.Expr.(type) {
		case *sqlparser.Subquery:
			subSQL := nodeToString(stmt.From[0])
			subSQL = subSQL[1:]
			subSQL = subSQL[:len(subSQL)-1]
			subQuery, err := Parse(subSQL)
			if err != nil {
				return fmt.Errorf("Unable to parse subquery: %v", err)
			}
			q.FromSubQuery = subQuery
			return nil
		case *sqlparser.TableName:
			q.From = strings.ToLower(string(e.Name))
			return nil
		}
	}
	return fmt.Errorf("Unknown from expression of type %v", reflect.TypeOf(stmt.From[0]))
}

func (q *Query) applyWhere(stmt *sqlparser.Select) error {
	where, err := goExprFor(stmt.Where.Expr)
	if err != nil {
		return err
	}
	log.Tracef("Applying where: %v", where)
	q.Where = where
	q.WhereSQL = strings.TrimSpace(nodeToString(stmt.Where))
	return err
}

func (q *Query) applyTimeRange(stmt *sqlparser.Select) error {
	if stmt.TimeRange.From != "" {
		t, d, err := stringToTimeOrDuration(stmt.TimeRange.From)
		if err != nil {
			return fmt.Errorf("Bad TIMERANGE from: %v", err)
		}
		if !t.IsZero() {
			q.AsOf = t
		} else {
			q.AsOfOffset = d
		}
	}

	if stmt.TimeRange.To != "" {
		t, d, err := stringToTimeOrDuration(stmt.TimeRange.To)
		if err != nil {
			return fmt.Errorf("Bad TIMERANGE to: %v", err)
		}
		if !t.IsZero() {
			q.Until = t
		} else {
			q.UntilOffset = d
		}
	}

	return nil
}

func (q *Query) applyGroupBy(stmt *sqlparser.Select) error {
	groupedByAnything := false
	groupBy := make(map[string]core.GroupBy)
	var groupByNames []string
	for _, e := range stmt.GroupBy {
		groupedByAnything = true
		_, ok := e.(*sqlparser.StarExpr)
		if ok {
			q.GroupByAll = true
			continue
		}
		nse, ok := e.(*sqlparser.NonStarExpr)
		if !ok {
			return fmt.Errorf("Unexpected expression in Group By: %v", nodeToString(e))
		}
		fn, ok := nse.Expr.(*sqlparser.FuncExpr)
		if ok && strings.EqualFold("PERIOD", string(fn.Name)) {
			log.Trace("Detected period in group by")
			if len(fn.Exprs) != 1 {
				return ErrInvalidPeriod
			}
			res, err := nodeToDuration(fn.Exprs[0])
			if err != nil {
				return err
			}
			q.Resolution = res
		} else if ok && strings.EqualFold("STRIDE", string(fn.Name)) {
			log.Trace("Detected stride in group by")
			if len(fn.Exprs) != 1 {
				return ErrInvalidStride
			}
			stride, err := nodeToDuration(fn.Exprs[0])
			if err != nil {
				return err
			}
			q.Stride = stride
		} else {
			var nestedEx sqlparser.Expr
			isCrosstab := ok && strings.HasPrefix(strings.ToUpper(string(fn.Name)), "CROSSTAB")
			if isCrosstab {
				log.Trace("Detected crosstab in group by")
				if len(fn.Exprs) < 1 {
					return ErrCROSSTABArity
				}
				if q.Crosstab != nil {
					return ErrCROSSTABUnique
				}
				nestedEx = fn
			} else {
				log.Trace("Dimension specified in group by")
				nestedEx = nse.Expr
			}
			ex, err := goExprFor(nestedEx)
			if err != nil {
				return err
			}
			if isCrosstab {
				q.Crosstab = ex
				q.CrosstabIncludesTotal = strings.HasSuffix(strings.ToUpper(string(fn.Name)), "T")
			} else {
				name := string(nse.As)
				if len(name) == 0 {
					cname, ok := nestedEx.(*sqlparser.ColName)
					if ok {
						name = string(cname.Name)
					}
				}
				if len(name) == 0 {
					return fmt.Errorf("Expression %v needs to be named via an AS", nodeToString(nse))
				}
				groupBy[name] = core.NewGroupBy(name, ex)
				groupByNames = append(groupByNames, name)
			}
		}
	}

	if !groupedByAnything {
		q.GroupByAll = true
	} else {
		// Important - GroupBy needs to be sorted alphabetically
		sort.Strings(groupByNames)
		for _, name := range groupByNames {
			q.GroupBy = append(q.GroupBy, groupBy[name])
		}
	}
	return nil
}

type havingClause struct {
	stmt *sqlparser.Select
	fielded
}

func (h *havingClause) Get(known core.Fields) (expr.Expr, error) {
	h.init(known)
	filter, _ := h.exprFor(h.stmt.Having.Expr, true)
	log.Tracef("Applying having: %v", filter)
	having := filter.(expr.Expr)
	err := having.Validate()
	if err != nil {
		return nil, fmt.Errorf("Invalid expression for HAVING clause: %v", err)
	}
	return having, nil
}

func (q *Query) applyOrderBy(stmt *sqlparser.Select) error {
	for _, _e := range stmt.OrderBy {
		field := nodeToString(_e.Expr)
		desc := strings.EqualFold("desc", _e.Direction)
		q.OrderBy = append(q.OrderBy, core.NewOrderBy(field, desc))
	}
	return nil
}

func (q *Query) applyLimit(stmt *sqlparser.Select) error {
	if stmt.Limit != nil {
		if stmt.Limit.Rowcount != nil {
			_limit := nodeToString(stmt.Limit.Rowcount)
			limit, err := strconv.Atoi(strings.ToLower(strings.Trim(_limit, "''")))
			if err != nil {
				return fmt.Errorf("Unable to parse limit %v: %v", _limit, err)
			}
			q.Limit = limit
		}

		if stmt.Limit.Offset != nil {
			_offset := nodeToString(stmt.Limit.Offset)
			offset, err := strconv.Atoi(strings.ToLower(strings.Trim(_offset, "''")))
			if err != nil {
				return fmt.Errorf("Unable to parse offset %v: %v", _offset, err)
			}
			q.Offset = offset
		}
	}

	return nil
}

func (f *fielded) exprFor(_e sqlparser.Expr, defaultToSum bool) (interface{}, error) {
	if log.IsTraceEnabled() {
		log.Tracef("Parsing expression of type %v: %v", reflect.TypeOf(_e), nodeToString(_e))
	}
	switch e := _e.(type) {
	case *sqlparser.ColName:
		return f.columnExprFor(e, defaultToSum)
	case *sqlparser.FuncExpr:
		fname := strings.ToUpper(string(e.Name))
		if fname == "IF" {
			return f.ifExprFor(e, fname, defaultToSum)
		}
		if fname == "BOUNDED" {
			return f.boundedExprFor(e, fname, defaultToSum)
		}
		if fname == "PERCENTILE" {
			return f.percentileExprFor(e, fname, defaultToSum)
		}
		if fname == "SHIFT" {
			return f.shiftExprFor(e, fname, defaultToSum)
		}
		switch len(e.Exprs) {
		case 1:
			return f.unaryFuncExprFor(e, fname, defaultToSum)
		case 2:
			return f.binaryFuncExprFor(e, fname, defaultToSum)
		default:
			return nil, ErrAggregateArity
		}

	case *sqlparser.ComparisonExpr:
		return f.comparisonExprFor(e, defaultToSum)
	case *sqlparser.BinaryExpr:
		return f.binaryExprFor(e, defaultToSum)
	case sqlparser.ValTuple:
		// For some reason addition comes through as a single element ValTuple, just
		// extract the first expression and continue.
		log.Tracef("Returning wrapped expression for ValTuple: %s", _e)
		return f.exprFor(e[0], defaultToSum)
	case *sqlparser.AndExpr:
		return f.andExprFor(e, defaultToSum)
	case *sqlparser.OrExpr:
		return f.orExprFor(e, defaultToSum)
	case *sqlparser.ParenBoolExpr:
		// TODO: make sure that we don't need to worry about parens in our
		// expression tree
		return f.exprFor(e.Expr, defaultToSum)
	case sqlparser.NumVal:
		fl, _ := strconv.ParseFloat(nodeToString(_e), 64)
		return expr.CONST(fl), nil
	default:
		str := nodeToString(_e)
		log.Tracef("Returning string for expression: %v", str)
		return str, nil
	}
}

func (f *fielded) columnExprFor(e *sqlparser.ColName, defaultToSum bool) (interface{}, error) {
	name := strings.ToLower(string(e.Name))
	if name == "_" {
		// This is a special name that stands for "value present"
		return expr.GT(core.PointsField.Expr, expr.CONST(0)), nil
	}

	// Default to a sum over the field
	ex := expr.FIELD(name)
	if !defaultToSum {
		return ex, nil
	}
	existing, found := f.fieldsMap[name]
	if found {
		// Use existing expression referenced by this name
		return existing.Expr, nil
	}
	return expr.SUM(ex), nil
}

func (f *fielded) ifExprFor(e *sqlparser.FuncExpr, fname string, defaultToSum bool) (interface{}, error) {
	if len(e.Exprs) != 2 {
		return nil, ErrIfArity
	}
	condEx, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	_valueEx, ok := e.Exprs[1].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	valueEx, valueErr := f.exprFor(_valueEx.Expr, true)
	if valueErr != nil {
		return nil, valueErr
	}
	boolEx, boolErr := goExprFor(condEx.Expr)
	if boolErr != nil {
		return nil, boolErr
	}
	return expr.IF(boolEx, valueEx), nil
}

func (f *fielded) boundedExprFor(e *sqlparser.FuncExpr, fname string, defaultToSum bool) (interface{}, error) {
	if len(e.Exprs) != 3 {
		return nil, ErrBoundedArity
	}
	param0, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	param1, ok := e.Exprs[1].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	param2, ok := e.Exprs[2].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	wrapped, err := f.exprFor(param0.Expr, defaultToSum)
	if err != nil {
		return nil, err
	}
	min, err := strconv.ParseFloat(nodeToString(param1.Expr), 64)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse min parameter to BOUNDED: %v", err)
	}
	max, err := strconv.ParseFloat(nodeToString(param2.Expr), 64)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse max parameter to BOUNDED: %v", err)
	}
	return expr.BOUNDED(wrapped, min, max), nil
}

func (f *fielded) percentileExprFor(e *sqlparser.FuncExpr, fname string, defaultToSum bool) (interface{}, error) {
	isOptimized := len(e.Exprs) == 2
	if len(e.Exprs) != 2 && len(e.Exprs) != 5 {
		return nil, ErrPercentileArity
	}

	_valueEx, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	var valueEx interface{}
	var valueField core.Field
	switch t := _valueEx.Expr.(type) {
	case *sqlparser.ColName:
		valueField = f.fieldsMap[strings.ToLower(string(t.Name))]
	}
	if expr.IsPercentile(valueField.Expr) {
		// existing field is a percentile, just wrap it
		valueEx = valueField.Expr
	} else {
		if isOptimized {
			// tried to wrap a non-percentile expression in a PERCENTILEOPT
			return nil, ErrPercentileOptWrap
		}
		// existing expression is not a percentile, need to get the field
		var valueErr error
		valueEx, valueErr = f.exprFor(_valueEx.Expr, false)
		if valueErr != nil {
			return nil, valueErr
		}
	}
	_percentileEx, ok := e.Exprs[1].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	percentileEx, percentileErr := f.exprFor(_percentileEx.Expr, false)
	if percentileErr != nil {
		return nil, percentileErr
	}

	if isOptimized {
		// don't bother with rest
		return expr.PERCENTILEOPT(valueEx, percentileEx), nil
	}

	min, err := nodeToFloat(e.Exprs[2])
	if err != nil {
		return nil, err
	}
	max, err := nodeToFloat(e.Exprs[3])
	if err != nil {
		return nil, err
	}
	precision, err := nodeToInt(e.Exprs[4])
	if err != nil {
		return nil, err
	}
	return expr.PERCENTILE(valueEx, percentileEx, min, max, int(precision)), nil
}

func (f *fielded) shiftExprFor(e *sqlparser.FuncExpr, fname string, defaultToSum bool) (interface{}, error) {
	if len(e.Exprs) != 2 {
		return nil, ErrShiftArity
	}
	_valueEx, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	valueEx, valueErr := f.exprFor(_valueEx.Expr, true)
	if valueErr != nil {
		return nil, valueErr
	}
	offset, offsetErr := nodeToDuration(e.Exprs[1])
	if offsetErr != nil {
		return nil, offsetErr
	}
	return expr.SHIFT(valueEx, offset), nil
}

func (f *fielded) unaryFuncExprFor(e *sqlparser.FuncExpr, fname string, defaultToSum bool) (interface{}, error) {
	var fn func(interface{}) (expr.Expr, error)
	_fn, ok := aggregateFuncs[fname]
	if ok {
		log.Tracef("Found function: %v", fname)
		defaultToSum = false
		fn = func(wrapped interface{}) (expr.Expr, error) {
			return _fn(wrapped), nil
		}
	} else {
		log.Tracef("Assuming unary math function: %v", fname)
		fn = func(wrapped interface{}) (expr.Expr, error) {
			return expr.UnaryMath(fname, wrapped)
		}
	}
	_param, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	se, err := f.exprFor(_param.Expr, defaultToSum)
	if err != nil {
		return nil, err
	}
	return fn(se)
}

func (f *fielded) binaryFuncExprFor(e *sqlparser.FuncExpr, fname string, defaultToSum bool) (interface{}, error) {
	fn, ok := binaryAggregateFuncs[fname]
	if !ok {
		return nil, fmt.Errorf("Unknown function '%v'", fname)
	}
	log.Tracef("Found function: %v", fname)
	_param1, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	_param2, ok := e.Exprs[1].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	se1, err := f.exprFor(_param1.Expr, false)
	if err != nil {
		return nil, err
	}
	se2, err := f.exprFor(_param2.Expr, false)
	if err != nil {
		return nil, err
	}
	return fn(se1, se2), nil
}

func (f *fielded) comparisonExprFor(e *sqlparser.ComparisonExpr, defaultToSum bool) (interface{}, error) {
	_op := string(e.Operator)
	if log.IsTraceEnabled() {
		log.Tracef("Parsing ComparisonExpr %v %v %v", nodeToString(e.Left), _op, nodeToString(e.Right))
	}
	cond, ok := conditions[_op]
	if !ok {
		return nil, fmt.Errorf("Unknown condition %v", _op)
	}
	left, err := f.exprFor(e.Left, true)
	if err != nil {
		return nil, err
	}
	right, err := f.exprFor(e.Right, true)
	if err != nil {
		return nil, err
	}
	return cond(left, right), nil
}

func (f *fielded) binaryExprFor(e *sqlparser.BinaryExpr, defaultToSum bool) (interface{}, error) {
	_op := string(e.Operator)
	if log.IsTraceEnabled() {
		log.Tracef("Parsing BinaryExpr %v %v %v", nodeToString(e.Left), _op, nodeToString(e.Right))
	}
	op, ok := operators[_op]
	if !ok {
		return nil, fmt.Errorf("Unknown operator %v", _op)
	}
	left, err := f.exprFor(e.Left, true)
	if err != nil {
		return nil, err
	}
	right, err := f.exprFor(e.Right, true)
	if err != nil {
		return nil, err
	}
	return op(left, right), nil
}

func (f *fielded) andExprFor(e *sqlparser.AndExpr, defaultToSum bool) (interface{}, error) {
	left, err := f.exprFor(e.Left, true)
	if err != nil {
		return "", err
	}
	right, err := f.exprFor(e.Right, true)
	if err != nil {
		return "", err
	}
	return expr.AND(left, right), nil
}

func (f *fielded) orExprFor(e *sqlparser.OrExpr, defaultToSum bool) (interface{}, error) {
	left, err := f.exprFor(e.Left, true)
	if err != nil {
		return "", err
	}
	right, err := f.exprFor(e.Right, true)
	if err != nil {
		return "", err
	}
	return expr.OR(left, right), nil
}

func goExprFor(_e sqlparser.Expr) (goexpr.Expr, error) {
	if log.IsTraceEnabled() {
		log.Tracef("Parsing goexpr of type %v: %v", reflect.TypeOf(_e), nodeToString(_e))
	}
	switch e := _e.(type) {
	case *sqlparser.AndExpr:
		left, err := goExprFor(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := goExprFor(e.Right)
		if err != nil {
			return nil, err
		}
		return goexpr.Binary("AND", left, right)
	case *sqlparser.OrExpr:
		left, err := goExprFor(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := goExprFor(e.Right)
		if err != nil {
			return nil, err
		}
		return goexpr.Binary("OR", left, right)
	case *sqlparser.ParenBoolExpr:
		return goExprFor(e.Expr)
	case *sqlparser.NotExpr:
		wrapped, err := goExprFor(e.Expr)
		if err != nil {
			return nil, err
		}
		return goexpr.Not(wrapped), nil
	case *sqlparser.ComparisonExpr:
		op := strings.ToUpper(e.Operator)
		left, err := goExprFor(e.Left)
		if err != nil {
			return nil, err
		}
		if op == "IN" {
			var right goexpr.List
			switch _right := e.Right.(type) {
			case sqlparser.ValTuple:
				list := make(goexpr.ArrayList, 0, len(_right))
				for _, ve := range _right {
					valE, valErr := goExprFor(ve)
					if valErr != nil {
						return nil, valErr
					}
					list = append(list, valE)
				}
				right = list
			case *sqlparser.Subquery:
				stmt, ok := _right.Select.(*sqlparser.Select)
				if !ok {
					return nil, fmt.Errorf("Subquery requires a SELECT statement")
				}
				_sq, parseErr := parse(stmt)
				if parseErr != nil {
					return nil, fmt.Errorf("In subquery %v: %v", nodeToString(stmt), parseErr)
				}
				fields, fieldsErr := _sq.Fields.Get(nil)
				if fieldsErr != nil {
					return nil, fieldsErr
				}
				numOfFieldsExcludingHaving := 0
				fieldName := ""
				for _, name := range fields.Names() {
					if name != "_having" {
						numOfFieldsExcludingHaving++
						fieldName = name
					}
				}
				if numOfFieldsExcludingHaving != 1 {
					return nil, fmt.Errorf("Subqueries in must select exactly 1 dimension")
				}
				right = &SubQuery{Dim: fieldName, SQL: nodeToString(stmt)}
			default:
				return nil, fmt.Errorf("IN requires a list of values on the right hand side, not %v %v", reflect.TypeOf(e.Right), nodeToString(e.Right))
			}
			return goexpr.In(left, right), nil
		}
		right, err := goExprFor(e.Right)
		if err != nil {
			return nil, err
		}
		return goexpr.Binary(op, left, right)
	case *sqlparser.ColName:
		colName := strings.TrimSpace(strings.ToLower(string(e.Name)))
		bl, err := strconv.ParseBool(colName)
		if err == nil {
			return goexpr.Constant(bl), nil
		}
		return goexpr.Param(colName), nil
	case sqlparser.StrVal:
		return goexpr.Constant(string(e)), nil
	case sqlparser.NumVal:
		intVal, parseErr := strconv.Atoi(string(e))
		if parseErr == nil {
			return goexpr.Constant(intVal), nil
		}
		floatVal, parseErr := strconv.ParseFloat(string(e), 64)
		if parseErr != nil {
			return nil, parseErr
		}
		return goexpr.Constant(floatVal), nil
	case *sqlparser.FuncExpr:
		fname := strings.ToUpper(string(e.Name))
		ge, err := goFnExprFor(e, fname)
		if err != nil && fname[0] == 'P' {
			log.Errorf("Error parsing function %v, looking for pushdown: %v", fname, err)
			// this might be a pushdown, try without the leading P
			ge, err = goFnExprFor(e, fname[1:])
			if err == nil {
				ge = goexpr.P(ge)
			}
		}
		return ge, err
	case *sqlparser.NullCheck:
		wrapped, err := goExprFor(e.Expr)
		if err != nil {
			return nil, err
		}
		op := "=="
		if "is not null" == e.Operator {
			op = "<>"
		}
		return goexpr.Binary(op, wrapped, goexpr.Constant(nil))
	default:
		return nil, fmt.Errorf("Unknown dimensional expression of type %v: %v", reflect.TypeOf(_e), nodeToString(_e))
	}
}

func goFnExprFor(e *sqlparser.FuncExpr, fname string) (goexpr.Expr, error) {
	alias, foundAlias := aliases[fname]
	if foundAlias {
		return applyAlias(e, alias)
	}
	numParams := len(e.Exprs)
	nfn, found := nullaryGoExpr[fname]
	if found {
		return nfn(), nil
	}
	ufn, found := unaryGoExpr[fname]
	if found {
		if numParams != 1 {
			return nil, fmt.Errorf("Function %v requires 1 parameter, not %d", fname, numParams)
		}
		p0, err := paramGoExpr(e, 0)
		if err != nil {
			return nil, err
		}
		return ufn(p0), nil
	}
	bfn, found := binaryGoExpr[fname]
	if found {
		if numParams != 2 {
			return nil, fmt.Errorf("Function %v requires 2 parameters, not %d", fname, numParams)
		}
		p0, err := paramGoExpr(e, 0)
		if err != nil {
			return nil, err
		}
		p1, err := paramGoExpr(e, 1)
		if err != nil {
			return nil, err
		}
		return bfn(p0, p1), nil
	}
	tfn, found := ternaryGoExpr[fname]
	if found {
		if numParams != 3 {
			return nil, fmt.Errorf("Function %v requires 3 parameters, not %d", fname, numParams)
		}
		p0, err := paramGoExpr(e, 0)
		if err != nil {
			return nil, err
		}
		p1, err := paramGoExpr(e, 1)
		if err != nil {
			return nil, err
		}
		p2, err := paramGoExpr(e, 2)
		if err != nil {
			return nil, err
		}
		return tfn(p0, p1, p2), nil
	}
	vfn, found := varGoExpr[fname]
	if found {
		params := make([]goexpr.Expr, 0, numParams)
		for i := 0; i < numParams; i++ {
			param, err := paramGoExpr(e, i)
			if err != nil {
				return nil, err
			}
			params = append(params, param)
		}
		return vfn(params...), nil
	}
	return nil, fmt.Errorf("Unknown function %v", fname)
}

func paramGoExpr(e *sqlparser.FuncExpr, idx int) (goexpr.Expr, error) {
	nse, ok := e.Exprs[idx].(*sqlparser.NonStarExpr)
	if !ok {
		return nil, ErrWildcardNotAllowed
	}
	return goExprFor(nse.Expr)
}

func applyAlias(e *sqlparser.FuncExpr, alias string) (goexpr.Expr, error) {
	parameterStrings := make([]interface{}, 0, len(e.Exprs))
	for _, pe := range e.Exprs {
		parameterStrings = append(parameterStrings, nodeToString(pe))
	}
	template := fmt.Sprintf("SELECT phcol FROM phtable GROUP BY %v AS phgb", alias)
	queryPlaceholder := fmt.Sprintf(template, parameterStrings...)
	qp, err := Parse(queryPlaceholder)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse query placeholder for applying alias: %v", err)
	}
	return qp.GroupBy[0].Expr, nil
}

func nodeToString(node sqlparser.SQLNode) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	node.Format(buf)
	return buf.String()
}

func stringToTimeOrDuration(str string) (time.Time, time.Duration, error) {
	t, err := time.Parse(time.RFC3339, str)
	if err == nil {
		return t, 0, err
	}
	d, err := ParseDuration(strings.ToLower(str))
	return t, d, err
}

func nodeToDuration(node sqlparser.SQLNode) (time.Duration, error) {
	str := nodeToString(node)
	dur, err := ParseDuration(strings.ToLower(strings.Replace(strings.Trim(str, "''"), " as ", "", 1)))
	if err != nil {
		err = fmt.Errorf("Unable to parse duration %v: %v", str, err)
	}
	return dur, err
}

func nodeToFloat(node sqlparser.SQLNode) (float64, error) {
	str := nodeToString(node)
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		err = fmt.Errorf("Unable to parse float %v: %v", str, err)
	}
	return val, err
}

func nodeToInt(node sqlparser.SQLNode) (int64, error) {
	str := nodeToString(node)
	val, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		err = fmt.Errorf("Unable to parse float %v: %v", str, err)
	}
	return val, err
}
