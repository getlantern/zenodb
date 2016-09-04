// Package sql provides the ability to parse SQL queries.
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
	"github.com/getlantern/golog"
	"github.com/getlantern/sqlparser"
	"github.com/getlantern/zenodb/expr"
)

var (
	log = golog.LoggerFor("zenodb.sql")
)
var (
	ErrSelectNoName       = errors.New("All expressions in SELECT must either reference a column name or include an AS alias")
	ErrIFArity            = errors.New("The IF function requires two parameters, like IF(dim = 1, SUM(b))")
	ErrCROSSTABArity      = fmt.Errorf("CROSSTAB allows only one argument")
	ErrAggregateArity     = errors.New("Aggregate functions take only one parameter, like SUM(b)")
	ErrWildcardNotAllowed = errors.New("Wildcard * is not supported")
	ErrNestedFunctionCall = errors.New("Nested function calls are not currently supported in SELECT")
	ErrInvalidPeriod      = errors.New("Please specify a period in the form period(5s) where 5s can be any valid Go duration expression")
)

var aggregateFuncs = map[string]func(interface{}) expr.Expr{
	"SUM":   expr.SUM,
	"MIN":   expr.MIN,
	"MAX":   expr.MAX,
	"COUNT": expr.COUNT,
	"AVG":   expr.AVG,
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

var unaryGoExpr = map[string]func(goexpr.Expr) goexpr.Expr{
	"CITY":         geo.CITY,
	"REGION":       geo.REGION,
	"REGION_CITY":  geo.REGION_CITY,
	"COUNTRY_CODE": geo.COUNTRY_CODE,
	"ISP":          isp.ISP,
	"ASN":          isp.ASN,
}

var varGoExpr = map[string]func(...goexpr.Expr) goexpr.Expr{
	"CONCAT": goexpr.Concat,
}

// Field is a named Expr.
type Field struct {
	Expr expr.Expr
	Name string
}

// NewField is a convenience method for creating new Fields.
func NewField(name string, ex expr.Expr) Field {
	return Field{
		Expr: ex,
		Name: name,
	}
}

func (f Field) String() string {
	return fmt.Sprintf("%v (%v)", f.Name, f.Expr)
}

// GroupBy is a named goexpr.Expr.
type GroupBy struct {
	Expr goexpr.Expr
	Name string
}

// NewGroupBy is a convenience method for creating new Fields.
func NewGroupBy(name string, ex goexpr.Expr) GroupBy {
	return GroupBy{
		Expr: ex,
		Name: name,
	}
}

func (g GroupBy) String() string {
	return fmt.Sprintf("%v (%v)", g.Name, g.Expr)
}

// Order represents an element in the ORDER BY clause such as "field DESC".
type Order struct {
	Field      string
	Descending bool
}

// Query represents the result of parsing a SELECT query.
type Query struct {
	// Fields are the fields from the SELECT clause in the order they appear.
	Fields []Field
	// From is the Table from the FROM clause
	From        string
	Resolution  time.Duration
	Where       goexpr.Expr
	AsOf        time.Time
	AsOfOffset  time.Duration
	Until       time.Time
	UntilOffset time.Duration
	// GroupBy are the GroupBy expressions ordered alphabetically by name.
	GroupBy    []GroupBy
	GroupByAll bool
	// Crosstab is the goexpr.Expr used for crosstabs (goes into columns rather than rows)
	Crosstab  goexpr.Expr
	Having    expr.Expr
	OrderBy   []Order
	Offset    int
	Limit     int
	fieldsMap map[string]Field
}

// TableFor returns the table in the FROM clause of this query
func TableFor(sql string) (string, error) {
	parsed, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}
	stmt := parsed.(*sqlparser.Select)
	return strings.ToLower(exprToString(stmt.From[0])), nil
}

// Parse parses a SQL statement and returns a corresponding *Query object.
func Parse(sql string, knownFields ...Field) (*Query, error) {
	q := &Query{
		fieldsMap: make(map[string]Field),
	}
	for _, field := range knownFields {
		q.fieldsMap[field.Name] = field
	}
	parsed, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	stmt := parsed.(*sqlparser.Select)
	if len(stmt.SelectExprs) > 0 {
		err = q.applySelect(stmt, knownFields)
		if err != nil {
			return nil, err
		}
	}
	if len(stmt.From) > 0 {
		err = q.applyFrom(stmt)
		if err != nil {
			return nil, err
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
	err = q.applyHaving(stmt)
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

func (q *Query) applySelect(stmt *sqlparser.Select, knownFields []Field) error {
	for _, _e := range stmt.SelectExprs {
		if exprToString(_e) == "_" {
			// Ignore underscore
			continue
		}
		switch e := _e.(type) {
		case *sqlparser.StarExpr:
			for _, field := range knownFields {
				q.addField(field)
			}
		case *sqlparser.NonStarExpr:
			if len(e.As) == 0 {
				col, isColName := e.Expr.(*sqlparser.ColName)
				if !isColName {
					return ErrSelectNoName
				}
				e.As = col.Name
			}
			_fe, err := q.exprFor(e.Expr, true)
			if err != nil {
				return err
			}
			fe, ok := _fe.(expr.Expr)
			if !ok {
				return fmt.Errorf("Not an Expr: %v", _fe)
			}
			err = fe.Validate()
			if err != nil {
				return fmt.Errorf("Invalid expression for '%s': %v", e.As, err)
			}
			q.addField(Field{fe.(expr.Expr), strings.ToLower(string(e.As))})
		}
	}

	return nil
}

func (q *Query) addField(field Field) {
	fieldAlreadySelected := false
	for _, existingField := range q.Fields {
		if existingField.Name == field.Name {
			fieldAlreadySelected = true
			break
		}
	}
	if !fieldAlreadySelected {
		q.Fields = append(q.Fields, field)
	}
	q.fieldsMap[field.Name] = field
}

func (q *Query) applyFrom(stmt *sqlparser.Select) error {
	q.From = strings.ToLower(exprToString(stmt.From[0]))
	return nil
}

func (q *Query) applyWhere(stmt *sqlparser.Select) error {
	where, err := q.goExprFor(stmt.Where.Expr)
	if err != nil {
		return err
	}
	log.Tracef("Applying where: %v", where)
	q.Where = where
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
	groupBy := make(map[string]GroupBy)
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
			return fmt.Errorf("Unexpected expression in Group By: %v", exprToString(e))
		}
		fn, ok := nse.Expr.(*sqlparser.FuncExpr)
		if ok && strings.EqualFold("PERIOD", string(fn.Name)) {
			log.Trace("Detected period in group by")
			if len(fn.Exprs) != 1 {
				return ErrInvalidPeriod
			}
			period := exprToString(fn.Exprs[0])
			res, err := time.ParseDuration(strings.ToLower(strings.Replace(strings.Trim(period, "''"), " as ", "", 1)))
			if err != nil {
				return fmt.Errorf("Unable to parse period %v: %v", period, err)
			}
			q.Resolution = res
		} else {
			var nestedEx sqlparser.Expr
			isCrosstab := ok && strings.EqualFold("CROSSTAB", string(fn.Name))
			if isCrosstab {
				log.Trace("Detected crosstab in group by")
				if len(fn.Exprs) != 1 {
					return ErrCROSSTABArity
				}
				_subEx, ok := fn.Exprs[0].(*sqlparser.NonStarExpr)
				if !ok {
					return ErrWildcardNotAllowed
				}
				nestedEx = _subEx.Expr
			} else {
				log.Trace("Dimension specified in group by")
				nestedEx = nse.Expr
			}
			ex, err := q.goExprFor(nestedEx)
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
					return fmt.Errorf("Expression %v needs to be named via an AS", exprToString(nse))
				}
				groupBy[name] = NewGroupBy(name, ex)
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

func (q *Query) applyHaving(stmt *sqlparser.Select) error {
	if stmt.Having != nil {
		filter, _ := q.exprFor(stmt.Having.Expr, true)
		log.Tracef("Applying having: %v", filter)
		q.Having = filter.(expr.Expr)
		err := q.Having.Validate()
		if err != nil {
			return fmt.Errorf("Invalid expression for HAVING clause: %v", err)
		}
	}
	return nil
}

func (q *Query) applyOrderBy(stmt *sqlparser.Select) error {
	for _, _e := range stmt.OrderBy {
		field := exprToString(_e.Expr)
		desc := strings.EqualFold("desc", _e.Direction)
		q.OrderBy = append(q.OrderBy, Order{field, desc})
	}
	return nil
}

func (q *Query) applyLimit(stmt *sqlparser.Select) error {
	if stmt.Limit != nil {
		if stmt.Limit.Rowcount != nil {
			_limit := exprToString(stmt.Limit.Rowcount)
			limit, err := strconv.Atoi(strings.ToLower(strings.Trim(_limit, "''")))
			if err != nil {
				return fmt.Errorf("Unable to parse limit %v: %v", _limit, err)
			}
			q.Limit = limit
		}

		if stmt.Limit.Offset != nil {
			_offset := exprToString(stmt.Limit.Offset)
			offset, err := strconv.Atoi(strings.ToLower(strings.Trim(_offset, "''")))
			if err != nil {
				return fmt.Errorf("Unable to parse offset %v: %v", _offset, err)
			}
			q.Offset = offset
		}
	}

	return nil
}

func (q *Query) exprFor(_e sqlparser.Expr, defaultToSum bool) (interface{}, error) {
	if log.IsTraceEnabled() {
		log.Tracef("Parsing expression of type %v: %v", reflect.TypeOf(_e), exprToString(_e))
	}
	switch e := _e.(type) {
	case *sqlparser.ColName:
		name := strings.ToLower(string(e.Name))
		f, found := q.fieldsMap[name]
		if found {
			// Use existing expression referenced by this name
			return f.Expr, nil
		}
		// Default to a sum over the field
		ex := expr.FIELD(name)
		if defaultToSum {
			ex = expr.SUM(ex)
		}
		return ex, nil
	case *sqlparser.FuncExpr:
		fname := strings.ToUpper(string(e.Name))
		if fname == "IF" {
			if len(e.Exprs) != 2 {
				return nil, ErrIFArity
			}
			condEx, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
			if !ok {
				return nil, ErrWildcardNotAllowed
			}
			_valueEx, ok := e.Exprs[1].(*sqlparser.NonStarExpr)
			if !ok {
				return nil, ErrWildcardNotAllowed
			}
			valueEx, valueErr := q.exprFor(_valueEx.Expr, true)
			if valueErr != nil {
				return nil, valueErr
			}
			boolEx, boolErr := q.goExprFor(condEx.Expr)
			if boolErr != nil {
				return nil, boolErr
			}
			return expr.IF(boolEx, valueEx)
		}
		if len(e.Exprs) != 1 {
			return nil, ErrAggregateArity
		}
		f, ok := aggregateFuncs[fname]
		if !ok {
			return nil, fmt.Errorf("Unknown function '%v'", fname)
		}
		log.Tracef("Found function: %v", fname)
		_param, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
		if !ok {
			return nil, ErrWildcardNotAllowed
		}
		return f(exprToString(_param.Expr)), nil
	case *sqlparser.ComparisonExpr:
		_op := string(e.Operator)
		if log.IsTraceEnabled() {
			log.Tracef("Parsing ComparisonExpr %v %v %v", exprToString(e.Left), _op, exprToString(e.Right))
		}
		cond, ok := conditions[_op]
		if !ok {
			return nil, fmt.Errorf("Unknown condition %v", _op)
		}
		left, err := q.exprFor(e.Left, true)
		if err != nil {
			return nil, err
		}
		right, err := q.exprFor(e.Right, true)
		if err != nil {
			return nil, err
		}
		return cond(left, right), nil
	case *sqlparser.BinaryExpr:
		_op := string(e.Operator)
		if log.IsTraceEnabled() {
			log.Tracef("Parsing BinaryExpr %v %v %v", exprToString(e.Left), _op, exprToString(e.Right))
		}
		op, ok := operators[_op]
		if !ok {
			return nil, fmt.Errorf("Unknown operator %v", _op)
		}
		left, err := q.exprFor(e.Left, true)
		if err != nil {
			return nil, err
		}
		right, err := q.exprFor(e.Right, true)
		if err != nil {
			return nil, err
		}
		return op(left, right), nil
	case sqlparser.ValTuple:
		// For some reason addition comes through as a single element ValTuple, just
		// extract the first expression and continue.
		log.Tracef("Returning wrapped expression for ValTuple: %s", _e)
		return q.exprFor(e[0], defaultToSum)
	case *sqlparser.AndExpr:
		left, err := q.exprFor(e.Left, true)
		if err != nil {
			return "", err
		}
		right, err := q.exprFor(e.Right, true)
		if err != nil {
			return "", err
		}
		return expr.AND(left, right), nil
	case *sqlparser.OrExpr:
		left, err := q.exprFor(e.Left, true)
		if err != nil {
			return "", err
		}
		right, err := q.exprFor(e.Right, true)
		if err != nil {
			return "", err
		}
		return expr.OR(left, right), nil
	case *sqlparser.ParenBoolExpr:
		// TODO: make sure that we don't need to worry about parens in our
		// expression tree
		return q.exprFor(e.Expr, defaultToSum)
	default:
		str := exprToString(_e)
		log.Tracef("Returning string for expression: %v", str)
		return str, nil
	}
}

func (q *Query) goExprFor(_e sqlparser.Expr) (goexpr.Expr, error) {
	if log.IsTraceEnabled() {
		log.Tracef("Parsing goexpr of type %v: %v", reflect.TypeOf(_e), exprToString(_e))
	}
	switch e := _e.(type) {
	case *sqlparser.AndExpr:
		left, err := q.goExprFor(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.goExprFor(e.Right)
		if err != nil {
			return nil, err
		}
		return goexpr.Binary("AND", left, right)
	case *sqlparser.OrExpr:
		left, err := q.goExprFor(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.goExprFor(e.Right)
		if err != nil {
			return nil, err
		}
		return goexpr.Binary("OR", left, right)
	case *sqlparser.ParenBoolExpr:
		return q.goExprFor(e.Expr)
	case *sqlparser.NotExpr:
		wrapped, err := q.goExprFor(e.Expr)
		if err != nil {
			return nil, err
		}
		return goexpr.Not(wrapped), nil
	case *sqlparser.ComparisonExpr:
		op := strings.ToUpper(e.Operator)
		left, err := q.goExprFor(e.Left)
		if err != nil {
			return nil, err
		}
		if op == "IN" {
			_right, ok := e.Right.(sqlparser.ValTuple)
			if !ok {
				return nil, fmt.Errorf("IN requires a list of values on the right hand side")
			}
			right := make([]goexpr.Expr, 0, len(_right))
			for _, ve := range _right {
				valE, err := q.goExprFor(ve)
				if err != nil {
					return nil, err
				}
				right = append(right, valE)
			}
			return goexpr.In(left, right...), nil
		}
		right, err := q.goExprFor(e.Right)
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
		val, parseErr := strconv.ParseFloat(string(e), 64)
		if parseErr != nil {
			return nil, parseErr
		}
		return goexpr.Constant(val), nil
	case *sqlparser.FuncExpr:
		fname := strings.ToUpper(string(e.Name))
		switch len(e.Exprs) {
		case 1:
			fn, found := unaryGoExpr[fname]
			if !found {
				return nil, fmt.Errorf("Unknown function %v", fname)
			}
			nse, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
			if !ok {
				return nil, ErrWildcardNotAllowed
			}
			wrapped, err := q.goExprFor(nse.Expr)
			if err != nil {
				return nil, err
			}
			return fn(wrapped), nil
		default:
			fn, found := varGoExpr[fname]
			if !found {
				return nil, fmt.Errorf("Unknown function %v", fname)
			}
			exprs := make([]goexpr.Expr, 0, len(e.Exprs))
			for _, _ex := range e.Exprs {
				nse, ok := _ex.(*sqlparser.NonStarExpr)
				if !ok {
					return nil, ErrWildcardNotAllowed
				}
				ex, err := q.goExprFor(nse.Expr)
				if err != nil {
					return nil, err
				}
				exprs = append(exprs, ex)
			}
			return fn(exprs...), nil
		}
	case *sqlparser.NullCheck:
		wrapped, err := q.goExprFor(e.Expr)
		if err != nil {
			return nil, err
		}
		op := "=="
		if "is not null" == e.Operator {
			op = "<>"
		}
		return goexpr.Binary(op, wrapped, nil)
	default:
		return nil, fmt.Errorf("Unknown dimensional expression of type %v: %v", reflect.TypeOf(_e), exprToString(_e))
	}
}

func exprToString(node sqlparser.SQLNode) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	node.Format(buf)
	return buf.String()
}

func stringToTimeOrDuration(str string) (time.Time, time.Duration, error) {
	t, err := time.Parse(time.RFC3339, str)
	if err == nil {
		return t, 0, err
	}
	d, err := time.ParseDuration(strings.ToLower(str))
	return t, d, err
}
