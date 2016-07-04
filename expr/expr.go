package expr

import (
	"fmt"
	"reflect"
	"strconv"

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

	String() string
}

func exprFor(expr interface{}) Expr {
	switch e := expr.(type) {
	case Expr:
		return e
	case string:
		v, err := strconv.ParseFloat(e, 64)
		if err == nil {
			return CONST(v)
		}
		return FIELD(e)
	case int:
		return CONST(float64(e))
	case int64:
		return CONST(float64(e))
	case int32:
		return CONST(float64(e))
	case int16:
		return CONST(float64(e))
	case byte:
		return CONST(float64(e))
	case float32:
		return CONST(float64(e))
	case float64:
		return CONST(e)
	default:
		panic(fmt.Sprintf("Got a %v, please specify an Expr, string, float64 or integer", reflect.TypeOf(expr)))
	}
}
