package sql

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/getlantern/golog"
	"github.com/getlantern/tdb/expr"
	"github.com/oxtoacart/sqlparser"
)

var (
	log = golog.LoggerFor("sql")
)
var (
	ErrSelectInvalidExpression = errors.New("All expressions in SELECT clause must be unary function calls like SUM(field) AS alias")
	ErrNonUnaryFunction        = errors.New("Currently only unary functions like SUM(field) are supported")
	ErrWildcardNotAllowed      = errors.New("Wildcard * is not supported")
	ErrNestedFunctionCall      = errors.New("Nested function calls are not currently supported in SELECT")
	ErrInvalidPeriod           = errors.New("Please specify a period in the form period(5s) where 5s can be any valid Go duration expression")
)

var unaryFuncs = map[string]func(param interface{}) expr.Expr{
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
	">=": expr.GTE,
	">":  expr.GT,
}

type Field struct {
	expr.Expr
	Name string
}

type Query struct {
	Fields      []Field
	From        string
	Resolution  time.Duration
	Where       string
	AsOf        time.Time
	AsOfOffset  time.Duration
	Until       time.Time
	UntilOffset time.Duration
	GroupBy     []string
	Having      expr.Expr
	OrderBy     []expr.Expr
	Offset      int
	Limit       int
}

// Parse parses a SQL statement and returns a corresponding *Query object.
func Parse(sql string) (*Query, error) {
	q := &Query{}
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
		log.Tracef("Parsing SELECT expression: %v", _e)
		e, ok := _e.(*sqlparser.NonStarExpr)
		if !ok {
			return ErrSelectInvalidExpression
		}
		if len(e.As) == 0 {
			return ErrSelectInvalidExpression
		}
		_fe, err := exprFor(e.Expr)
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
		q.Fields = append(q.Fields, Field{fe.(expr.Expr), strings.ToLower(string(e.As))})
	}

	return nil
}

func (q *Query) applyFrom(stmt *sqlparser.Select) error {
	q.From = strings.ToLower(exprToString(stmt.From[0]))
	return nil
}

func (q *Query) applyWhere(stmt *sqlparser.Select) error {
	where, err := boolExprFor(stmt.Where.Expr)
	if err != nil {
		return err
	}
	log.Tracef("Applying where: %v", where)
	q.Where = where
	return nil
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
	for _, e := range stmt.GroupBy {
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
	return nil
}

func (q *Query) applyHaving(stmt *sqlparser.Select) error {
	if stmt.Having != nil {
		filter, _ := exprFor(stmt.Having.Expr)
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
		e, err := exprFor(_e.Expr)
		if err != nil {
			return err
		}
		asc := strings.EqualFold("ASC", _e.Direction)
		if log.IsTraceEnabled() {
			log.Tracef("Ordering by %v asc?: %v", e, asc)
		}
		ex := e.(expr.Expr)
		if !asc {
			ex = expr.MULT(-1, ex)
		}
		q.OrderBy = append(q.OrderBy, ex)
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

func exprFor(_e sqlparser.Expr) (interface{}, error) {
	if log.IsTraceEnabled() {
		log.Tracef("Parsing expression of type %v: %v", reflect.TypeOf(_e), exprToString(_e))
	}
	switch e := _e.(type) {
	case *sqlparser.FuncExpr:
		if len(e.Exprs) != 1 {
			return nil, ErrNonUnaryFunction
		}
		fname := strings.ToUpper(string(e.Name))
		f, ok := unaryFuncs[fname]
		if !ok {
			return nil, fmt.Errorf("Unknown function '%v'", fname)
		}
		log.Tracef("Found function: %v", fname)
		_param, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
		if !ok {
			return nil, ErrWildcardNotAllowed
		}
		param, err := exprFor(_param.Expr)
		if err != nil {
			return nil, err
		}
		return f(param), nil
	case *sqlparser.ComparisonExpr:
		_op := string(e.Operator)
		if log.IsTraceEnabled() {
			log.Tracef("Parsing ComparisonExpr %v %v %v", exprToString(e.Left), _op, exprToString(e.Right))
		}
		cond, ok := conditions[_op]
		if !ok {
			return nil, fmt.Errorf("Unknown condition %v", _op)
		}
		left, err := exprFor(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := exprFor(e.Right)
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
		left, err := exprFor(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := exprFor(e.Right)
		if err != nil {
			return nil, err
		}
		return op(left, right), nil
	case sqlparser.ValTuple:
		// For some reason addition comes through as a single element ValTuple, just
		// extract the first expression and continue.
		log.Tracef("Returning wrapped expression for ValTuple: %s", _e)
		return exprFor(e[0])
	default:
		str := exprToString(_e)
		log.Tracef("Returning string for expression: %v", str)
		return str, nil
	}
}

func boolExprFor(_e sqlparser.Expr) (string, error) {
	if log.IsTraceEnabled() {
		log.Tracef("Parsing boolean expression of type %v: %v", reflect.TypeOf(_e), exprToString(_e))
	}
	switch e := _e.(type) {
	case *sqlparser.AndExpr:
		left, err := boolExprFor(e.Left)
		if err != nil {
			return "", err
		}
		right, err := boolExprFor(e.Right)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s && %s", left, right), nil
	case *sqlparser.OrExpr:
		left, err := boolExprFor(e.Left)
		if err != nil {
			return "", err
		}
		right, err := boolExprFor(e.Right)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s || %s", left, right), nil
	case *sqlparser.ParenBoolExpr:
		wrapped, err := boolExprFor(e.Expr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s)", wrapped), nil
	case *sqlparser.NotExpr:
		wrapped, err := boolExprFor(e.Expr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("!%s", wrapped), nil
	case *sqlparser.ComparisonExpr:
		left, err := boolExprFor(e.Left)
		if err != nil {
			return "", err
		}
		right, err := boolExprFor(e.Right)
		if err != nil {
			return "", err
		}
		op := e.Operator
		switch strings.ToUpper(op) {
		case "LIKE":
			op = "=~"
		case "NOT LIKE":
			op = "!~"
		case "<>":
			op = "!="
		case "=":
			op = "=="
		case ">", ">=", "<", "<=", "!=":
			// Leave as is
		default:
			return "", fmt.Errorf("Unknown comparison operator '%v'", e.Operator)
		}
		return fmt.Sprintf("%s %s %s", left, op, right), nil
	default:
		str := exprToString(_e)
		log.Tracef("Returning string for boolean expression: %v", str)
		return str, nil
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
