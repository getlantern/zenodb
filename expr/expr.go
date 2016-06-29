package expr

import (
	"github.com/getlantern/golog"
	"github.com/oxtoacart/govaluate"
	"github.com/oxtoacart/tdb/values"
)

var (
	log = golog.LoggerFor("expr")
)

// Map is an implementation of the Parameters interface using a map.
type Map map[string]values.Value

// Get implements the method from Parameters
func (p Map) Get(name string) (interface{}, error) {
	val := p[name]
	if val == nil {
		return float64(0), nil
	}
	return val.Val(), nil
}

type Expr func(fields govaluate.Parameters) values.Value

func Constant(val values.Value) Expr {
	return func(fields govaluate.Parameters) values.Value {
		return val
	}
}
