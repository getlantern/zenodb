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

	PointsField = core.NewField("_points", expr.SUM("_point"))
)

var (
	ErrSelectNoName       = errors.New("All expressions in SELECT must either reference a column name or include an AS alias")
	ErrIfArity            = errors.New("IF requires two parameters, like IF(dim = 1, SUM(b))")
	ErrBoundedArity       = errors.New("BOUNDED requires three parameters, like BOUNDED(b, 0, 100)")
	ErrShiftArity         = errors.New("SHIFT requires two parameters, like SHIFT(SUM(b), '1h')")
	ErrCROSSTABArity      = errors.New("CROSSTAB requires at least one argument")
	ErrCROSSTABUnique     = errors.New("Only one CROSSTAB statement allowed per query")
	ErrAggregateArity     = errors.New("Aggregate functions take only one parameter, like SUM(b)")
	ErrWildcardNotAllowed = errors.New("Wildcard * is not supported")
	ErrNestedFunctionCall = errors.New("Nested function calls are not currently supported in SELECT")
	ErrInvalidPeriod      = errors.New("Please specify a period in the form period(5s) where 5s can be any valid Go duration expression")
	ErrInvalidStride      = errors.New("Please specify a stride in the form stride(5s) where 5s can be any valid Go duration expression")
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
	"HGET": redis.HGet,
}

var ternaryGoExpr = map[string]func(goexpr.Expr, goexpr.Expr, goexpr.Expr) goexpr.Expr{
	"SPLIT":  goexpr.Split,
	"SUBSTR": goexpr.Substr,
}

var varGoExpr = map[string]func(...goexpr.Expr) goexpr.Expr{
	"CONCAT": goexpr.Concat,
	"CROSSTAB": func(exprs ...goexpr.Expr) goexpr.Expr {
		// Crosstab expressions are concatenated using underscore
		allExprs := make([]goexpr.Expr, 1, len(exprs)+1)
		allExprs[0] = goexpr.Constant("_")
		allExprs = append(allExprs, exprs...)
		return goexpr.Concat(allExprs...)
	},
	"ANY": goexpr.Any,
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
	// Fields are the fields from the SELECT clause in the order they appear.
	Fields            core.FieldSource
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
	Shift        time.Duration
	// GroupBy are the GroupBy expressions ordered alphabetically by name.
	GroupBy    []core.GroupBy
	GroupByAll bool
	// Crosstab is the goexpr.Expr used for crosstabs (goes into columns rather than rows)
	Crosstab goexpr.Expr
	Having   core.ExprSource
	OrderBy  []core.OrderBy
	Offset   int
	Limit    int
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
	if len(stmt.SelectExprs) > 0 {
		q.Fields = &selectClause{
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
	if stmt.Having != nil {
		q.Having = &havingClause{
			stmt:    stmt,
			fielded: fielded{sql: nodeToString(stmt.Having.Expr)},
		}
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
			if len(e.As) == 0 {
				col, isColName := e.Expr.(*sqlparser.ColName)
				if !isColName {
					return nil, ErrSelectNoName
				}
				e.As = col.Name
			}
			_fe, err := s.exprFor(e.Expr, true)
			if err != nil {
				return nil, err
			}
			fe, ok := _fe.(expr.Expr)
			if !ok {
				return nil, fmt.Errorf("Not an Expr: %v", _fe)
			}
			err = fe.Validate()
			if err != nil {
				return nil, fmt.Errorf("Invalid expression for '%s': %v", e.As, err)
			}
			fields = s.addField(fields, core.NewField(strings.ToLower(string(e.As)), fe.(expr.Expr)))
		}
	}

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
			q.AsOf = subQuery.AsOf
			q.AsOfOffset = subQuery.AsOfOffset
			q.Until = subQuery.Until
			q.UntilOffset = subQuery.UntilOffset
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
			isCrosstab := ok && strings.EqualFold("CROSSTAB", string(fn.Name))
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
		name := strings.ToLower(string(e.Name))
		// Default to a sum over the field
		ex := expr.FIELD(name)
		if !defaultToSum {
			return ex, nil
		}
		f, found := f.fieldsMap[name]
		if found {
			// Use existing expression referenced by this name
			return f.Expr, nil
		}
		return expr.SUM(ex), nil
	case *sqlparser.FuncExpr:
		fname := strings.ToUpper(string(e.Name))
		if fname == "IF" {
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
		if fname == "BOUNDED" {
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
		if fname == "SHIFT" {
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
		switch len(e.Exprs) {
		case 1:
			fn, ok := aggregateFuncs[fname]
			if !ok {
				return nil, fmt.Errorf("Unknown function '%v'", fname)
			}
			log.Tracef("Found function: %v", fname)
			_param, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
			if !ok {
				return nil, ErrWildcardNotAllowed
			}
			se, err := f.exprFor(_param.Expr, false)
			if err != nil {
				return nil, err
			}
			return fn(se), nil
		case 2:
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
		default:
			return nil, ErrAggregateArity
		}

	case *sqlparser.ComparisonExpr:
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
	case *sqlparser.BinaryExpr:
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
	case sqlparser.ValTuple:
		// For some reason addition comes through as a single element ValTuple, just
		// extract the first expression and continue.
		log.Tracef("Returning wrapped expression for ValTuple: %s", _e)
		return f.exprFor(e[0], defaultToSum)
	case *sqlparser.AndExpr:
		left, err := f.exprFor(e.Left, true)
		if err != nil {
			return "", err
		}
		right, err := f.exprFor(e.Right, true)
		if err != nil {
			return "", err
		}
		return expr.AND(left, right), nil
	case *sqlparser.OrExpr:
		left, err := f.exprFor(e.Left, true)
		if err != nil {
			return "", err
		}
		right, err := f.exprFor(e.Right, true)
		if err != nil {
			return "", err
		}
		return expr.OR(left, right), nil
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
				if len(fields) != 1 {
					return nil, fmt.Errorf("Subqueries must select at exactly 1 dimension")
				}
				right = &SubQuery{Dim: fields[0].Name, SQL: nodeToString(stmt)}
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
