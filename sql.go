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
//  WHERE Dim_a =~ '^172.56.+' // this is a regex match
//  GROUP BY dim_A, period(5s) // time is a special function
//  ORDER BY AVG(Rate) ASC
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
	err = aq.applyGroupBy(stmt)
	if err != nil {
		return err
	}
	err = aq.applyOrderBy(stmt)
	if err != nil {
		return err
	}
	return nil
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

func (aq *Query) applyGroupBy(stmt *sqlparser.Select) error {
	for _, e := range stmt.GroupBy {
		fn, ok := e.(*sqlparser.FuncExpr)
		if ok && strings.EqualFold("period", string(fn.Name)) {
			log.Trace("Detected period in group by")
			if len(fn.Exprs) != 1 {
				return ErrInvalidPeriod
			}
			period := exprToString(fn.Exprs[0])
			res, err := time.ParseDuration(strings.ToLower(strings.Replace(period, " as ", "", 1)))
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
		aq.OrderBy(e.(expr.Expr), strings.EqualFold("ASC", _e.Direction))
	}
	return nil
}

func exprFor(se sqlparser.Expr) (interface{}, error) {
	if log.IsTraceEnabled() {
		log.Tracef("Parsing expression of type %v: %v", reflect.TypeOf(se), exprToString(se))
	}
	switch e := se.(type) {
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
		str := exprToString(se)
		log.Tracef("Returning string for expression: %v", str)
		return str, nil
	}
}

func exprToString(node sqlparser.SQLNode) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	node.Format(buf)
	return buf.String()
}
