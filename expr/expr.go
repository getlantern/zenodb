package expr

import (
	"github.com/getlantern/golog"
	"github.com/oxtoacart/tdb/values"
)

var (
	log = golog.LoggerFor("expr")
)

type Expr func(fields map[string]values.Value) values.Value

func Constant(val values.Value) Expr {
	return func(fields map[string]values.Value) values.Value {
		return val
	}
}
