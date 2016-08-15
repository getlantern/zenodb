package sql

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/expr"
	"github.com/oxtoacart/sqlparser"
)

var (
	log = golog.LoggerFor("zenodb.sql")
)
var (
	ErrSelectNoName       = errors.New("All expressions in SELECT must either reference a column name or include an AS alias")
	ErrIFArity            = errors.New("The IF function requires two parameters, like IF(dim = 1, SUM(b))")
	ErrAggregateArity     = errors.New("Aggregate functions take only one parameter, like SUM(b)")
	ErrWildcardNotAllowed = errors.New("Wildcard * is not supported")
	ErrNestedFunctionCall = errors.New("Nested function calls are not currently supported in SELECT")
	ErrInvalidPeriod      = errors.New("Please specify a period in the form period(5s) where 5s can be any valid Go duration expression")
)

var aggregateFuncs = map[string]func(param interface{}) expr.Expr{
	"SUM":   expr.SUM,
	"COUNT": expr.COUNT,
	"AVG":   expr.AVG,
}

var operators = map[string]func(left interface{}, right interface{}) expr.Expr{
	"+": expr.ADD,
	"-": expr.SUB,
	"*": expr.MULT,
	"/": expr.DIV,
}

var conditions = map[string]func(left interface{}, right interface{}) expr.Expr{
	"<":  expr.LT,
	"<=": expr.LTE,
	"=":  expr.EQ,
	"<>": expr.NEQ,
	"!=": expr.NEQ,
	">=": expr.GTE,
	">":  expr.GT,
}

type Field struct {
	Expr expr.Expr
	Name string
}

func (f Field) String() string {
	return fmt.Sprintf("%v (%v)", f.Name, f.Expr)
}

type Order struct {
	Field      string
	Descending bool
}

type Query struct {
	Fields      []Field
	From        string
	Resolution  time.Duration
	Where       goexpr.Expr
	AsOf        time.Time
	AsOfOffset  time.Duration
	Until       time.Time
	UntilOffset time.Duration
	GroupBy     []string
	GroupByAll  bool
	Having      expr.Expr
	OrderBy     []Order
	Offset      int
	Limit       int
	fieldsMap   map[string]Field
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
		err = q.applySelect(stmt)
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

func (q *Query) applySelect(stmt *sqlparser.Select) error {
	for _, _e := range stmt.SelectExprs {
		e, ok := _e.(*sqlparser.NonStarExpr)
		if !ok {
			return ErrWildcardNotAllowed
		}
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
		field := Field{fe.(expr.Expr), strings.ToLower(string(e.As))}
		q.Fields = append(q.Fields, field)
		q.fieldsMap[field.Name] = field
	}

	return nil
}

func (q *Query) applyFrom(stmt *sqlparser.Select) error {
	q.From = strings.ToLower(exprToString(stmt.From[0]))
	return nil
}

func (q *Query) applyWhere(stmt *sqlparser.Select) error {
	where, err := q.boolExprFor(stmt.Where.Expr)
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
	for _, e := range stmt.GroupBy {
		groupedByAnything = true
		_, ok := e.(*sqlparser.StarExpr)
		if ok {
			q.GroupByAll = true
			continue
		}
		fn, ok := e.(*sqlparser.FuncExpr)
		if ok && strings.EqualFold("period", string(fn.Name)) {
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
			log.Trace("Dimension specified in group by")
			q.GroupBy = append(q.GroupBy, strings.ToLower(exprToString(e)))
		}
	}

	if !groupedByAnything {
		q.GroupByAll = true
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
			boolEx, boolErr := q.boolExprFor(condEx.Expr)
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

func (q *Query) boolExprFor(_e sqlparser.Expr) (goexpr.Expr, error) {
	if log.IsTraceEnabled() {
		log.Tracef("Parsing boolean expression of type %v: %v", reflect.TypeOf(_e), exprToString(_e))
	}
	switch e := _e.(type) {
	case *sqlparser.AndExpr:
		left, err := q.boolExprFor(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.boolExprFor(e.Right)
		if err != nil {
			return nil, err
		}
		return goexpr.Binary("AND", left, right)
	case *sqlparser.OrExpr:
		left, err := q.boolExprFor(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.boolExprFor(e.Right)
		if err != nil {
			return nil, err
		}
		return goexpr.Binary("OR", left, right)
	case *sqlparser.ParenBoolExpr:
		return q.boolExprFor(e.Expr)
	case *sqlparser.NotExpr:
		wrapped, err := q.boolExprFor(e.Expr)
		if err != nil {
			return nil, err
		}
		return goexpr.Not(wrapped), nil
	case *sqlparser.ComparisonExpr:
		left, err := q.boolExprFor(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.boolExprFor(e.Right)
		if err != nil {
			return nil, err
		}
		return goexpr.Binary(strings.ToUpper(e.Operator), left, right)
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
	default:
		return nil, fmt.Errorf("Unknown boolean expression of type %v: %v", reflect.TypeOf(_e), exprToString(_e))
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
