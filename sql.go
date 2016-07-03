package tdb

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/oxtoacart/tdb/expr"
	"github.com/xwb1989/sqlparser"
)

var (
	ErrMissingSelect           = errors.New("Please SELECT at least one field")
	ErrInvalidFrom             = errors.New("Please specify a single table in the FROM clause")
	ErrSelectInvalidExpression = errors.New("All expressions in SELECT clause must be unary function calls like SUM(field) AS alias")
	ErrNonUnaryFunction        = errors.New("Currently only unary functions like SUM(field) are supported")
	ErrWildcardNotAllowed      = errors.New("Wildcard * is not supported")
	ErrNestedFunctionCall      = errors.New("Nested function calls are not currently supported in SELECT")
	ErrInvalidPeriod           = errors.New("Please specify a period in the form period(5s) where 5s can be any valid Go duration expression")
)

var unaryFuncs = map[string]func(param interface{}) expr.Expr{
	"SUM":   expr.SUM,
	"MIN":   expr.MIN,
	"MAX":   expr.MAX,
	"COUNT": expr.COUNT,
	"AVG":   expr.AVG,
}

var operators = map[string]func(left interface{}, right interface{}) expr.Expr{
	"+": expr.ADD,
	"-": expr.SUB,
	"*": expr.MULT,
	"/": expr.DIV,
}

// applySQL parses a SQL statement and populates query parameters using it.
// For example:
//
//  SELECT AVG(a / (A + b + C)) AS rate
//  FROM Table_A
//  WHERE Dim_a LIKE '172.56.' AND dim_b > 10
//  GROUP BY dim_A, period(5s) // time is a special function
//  ORDER BY AVG(Rate) DESC
//
func (aq *Query) applySQL(sql string) error {
	parsed, err := sqlparser.Parse(sql)
	if err != nil {
		return err
	}
	stmt := parsed.(*sqlparser.Select)
	if len(stmt.SelectExprs) == 0 {
		return ErrMissingSelect
	}
	if len(stmt.From) != 1 {
		return ErrInvalidFrom
	}
	err = aq.applySelect(stmt)
	if err != nil {
		return err
	}
	err = aq.applyFrom(stmt)
	if err != nil {
		return err
	}
	if stmt.Where != nil {
		err = aq.applyWhere(stmt)
		if err != nil {
			return err
		}
	}
	err = aq.applyGroupBy(stmt)
	if err != nil {
		return err
	}
	err = aq.applyOrderBy(stmt)
	if err != nil {
		return err
	}
	return aq.applyLimit(stmt)
}

func (aq *Query) applySelect(stmt *sqlparser.Select) error {
	for _, _e := range stmt.SelectExprs {
		log.Tracef("Parsing SELECT expression: %v", _e)
		e, ok := _e.(*sqlparser.NonStarExpr)
		if !ok {
			return ErrSelectInvalidExpression
		}
		if len(e.As) == 0 {
			return ErrSelectInvalidExpression
		}
		fe, err := exprFor(e.Expr)
		if err != nil {
			return err
		}
		aq.Select(string(e.As), fe.(expr.Expr))
	}

	return nil
}

func (aq *Query) applyFrom(stmt *sqlparser.Select) error {
	aq.table = strings.ToLower(exprToString(stmt.From[0]))
	return nil
}

func (aq *Query) applyWhere(stmt *sqlparser.Select) error {
	filter, _ := boolExprFor(stmt.Where.Expr)
	log.Tracef("Applying where: %v", filter)
	aq.Where(filter)
	return nil
}

func (aq *Query) applyGroupBy(stmt *sqlparser.Select) error {
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
			aq.Resolution(res)
		} else {
			log.Trace("Dimension specified in group by")
			aq.GroupBy(exprToString(e))
		}
	}
	return nil
}

func (aq *Query) applyOrderBy(stmt *sqlparser.Select) error {
	for _, _e := range stmt.OrderBy {
		e, err := exprFor(_e.Expr)
		if err != nil {
			return err
		}
		asc := strings.EqualFold("ASC", _e.Direction)
		if log.IsTraceEnabled() {
			log.Tracef("Ordering by %v asc?: %v", expr.ToString(e.(expr.Expr)), asc)
		}
		aq.OrderBy(e.(expr.Expr), asc)
	}
	return nil
}

func (aq *Query) applyLimit(stmt *sqlparser.Select) error {
	if stmt.Limit != nil {
		if stmt.Limit.Rowcount != nil {
			_limit := exprToString(stmt.Limit.Rowcount)
			limit, err := time.ParseDuration(strings.ToLower(strings.Trim(_limit, "''")))
			if err != nil {
				return fmt.Errorf("Unable to parse limit %v: %v", _limit, err)
			}
			aq.Limit(limit)
		}

		if stmt.Limit.Offset != nil {
			_offset := exprToString(stmt.Limit.Offset)
			offset, err := time.ParseDuration(strings.ToLower(strings.Trim(_offset, "''")))
			if err != nil {
				return fmt.Errorf("Unable to parse offset %v: %v", _offset, err)
			}
			aq.Offset(offset)
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
		log.Tracef("Found function in SELECT: %v", fname)
		_param, ok := e.Exprs[0].(*sqlparser.NonStarExpr)
		if !ok {
			return nil, ErrWildcardNotAllowed
		}
		param, err := exprFor(_param.Expr)
		if err != nil {
			return nil, err
		}
		return f(param), nil
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
		log.Tracef("Returning wrapped expression for ValTuple")
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
		case "<>":
			op = "!="
		case "=":
			op = "=="
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
