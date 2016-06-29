package expr

import (
	"github.com/oxtoacart/govaluate"
	"github.com/oxtoacart/tdb/values"
)

func Calc(expression string) Expr {
	e, err := govaluate.NewEvaluableExpression(expression)
	if err != nil {
		log.Errorf("Unable to parse expression %v: %v", expression, err)
		return Constant(values.Float(0))
	}
	return func(fields govaluate.Parameters) values.Value {
		result, err := e.Eval(fields)
		if err != nil {
			log.Errorf("Unable to evaluate expression %v: %v", expression, err)
			return values.Float(0)
		}
		return values.Float(result.(float64))
	}
}
