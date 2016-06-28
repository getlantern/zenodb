package calc

import (
	"github.com/Knetic/govaluate"
	"github.com/oxtoacart/tdb/values"
)

type expr struct {
	e *govaluate.EvaluableExpression
}

func Expr(expression string) (Calculated, error) {
	e, err := govaluate.NewEvaluableExpression(expression)
	if err != nil {
		return nil, err
	}
	return &expr{e}, nil
}

func (e *expr) Initial() values.Value {
	return values.Float(0)
}

func (e *expr) Add(current values.Value, otherFields map[string]interface{}) (values.Value, error) {
	result, err := e.e.Evaluate(otherFields)
	if err != nil {
		return current, err
	}
	return current.Add(result.(float64)), nil
}
