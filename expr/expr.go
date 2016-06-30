package expr

import (
	"github.com/getlantern/golog"
)

var (
	log = golog.LoggerFor("expr")
)

type Value interface {
	Get() float64
}

type Float float64

func (f Float) Get() float64 {
	return float64(f)
}

type Params interface {
	Get(name string) Value
}

// Map is an implementation of the Params interface using a map.
type Map map[string]Value

// Get implements the method from the Params interface
func (p Map) Get(name string) Value {
	val := p[name]
	if val == nil {
		val = Float(0)
	}
	return val
}

type Accumulator interface {
	Update(params Params)

	Get() float64
}

type Expr interface {
	Accumulator() Accumulator

	DependsOn() []string
}

func exprFor(expr interface{}) Expr {
	switch e := expr.(type) {
	case Expr:
		return e
	case string:
		return Field(e)
	case int:
		return Constant(float64(e))
	case float64:
		return Constant(e)
	default:
		panic("Please specify an Expr, string, float64 or integer")
	}
}
