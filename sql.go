package tdb

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/oxtoacart/tdb/expr"
	"github.com/xwb1989/sqlparser"
)

var (
	ErrSelectInvalidExpression = errors.New("All expressions in SELECT clause must be unary function calls like SUM(field) AS alias. Wildcard * is not supported.")
	ErrNestedFunctionCall      = errors.New("Nested function calls are not currently supported in SELECT")
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

// ApplySQL parses a SQL statement and populates query parameters using it.
// For example:
//
//    SELECT AVG(a / (a + b + c)) AS rate
//    FROM table_a
//    WHERE dim_a =~ '172.56.+'
//    GROUP BY dim_a, time(15s)
//    ORDER BY rate ASC
//
func (aq *Query) ApplySQL(sql string) (*Query, error) {
	parsed, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	stmt := parsed.(*sqlparser.Select)
	for _, _e := range stmt.SelectExprs {
		log.Tracef("Parsing SELECT expression: %v", _e)
		e, ok := _e.(*sqlparser.NonStarExpr)
		if !ok {
			return nil, ErrSelectInvalidExpression
		}
		if len(e.As) == 0 {
			return nil, ErrSelectInvalidExpression
		}
		fc, ok := e.Expr.(*sqlparser.FuncExpr)
		if !ok {
			return nil, ErrSelectInvalidExpression
		}
		if len(fc.Exprs) != 1 {
			return nil, ErrSelectInvalidExpression
		}
		fname := strings.ToUpper(string(fc.Name))
		f, ok := unaryFuncs[fname]
		if !ok {
			return nil, fmt.Errorf("Unknown function '%v'", fname)
		}
		log.Tracef("Found function in SELECT: %v", fname)
		_param, ok := fc.Exprs[0].(*sqlparser.NonStarExpr)
		if !ok {
			return nil, ErrSelectInvalidExpression
		}
		param, err := exprFor(_param.Expr)
		if err != nil {
			return nil, err
		}
		aq.Select(string(e.As), f(param))
	}
	return aq, nil
}

func exprFor(se sqlparser.Expr) (interface{}, error) {
	if log.IsTraceEnabled() {
		log.Tracef("Parsing expression of type %v: %v", reflect.TypeOf(se), toString(se))
	}
	switch e := se.(type) {
	case *sqlparser.BinaryExpr:
		_op := string(e.Operator)
		if log.IsTraceEnabled() {
			log.Tracef("Parsing BinaryExpr %v %v %v", toString(e.Left), _op, toString(e.Right))
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
		str := toString(se)
		log.Tracef("Returning string for expression: %v", str)
		return str, nil
	}
}

func toString(node sqlparser.SQLNode) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	node.Format(buf)
	return buf.String()
}
