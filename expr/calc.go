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
	return func(fields map[string]values.Value) values.Value {
		result, err := e.Eval(parameters(fields))
		if err != nil {
			log.Errorf("Unable to evaluate expression %v: %v", expression, err)
			return values.Float(0)
		}
		return values.Float(result.(float64))
	}
}

type parameters map[string]values.Value

func (p parameters) Get(name string) (interface{}, error) {
	val := p[name]
	if val == nil {
		return float64(0), nil
	}
	return val.Val(), nil
}
