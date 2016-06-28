package expr

import (
	"github.com/oxtoacart/tdb/values"
)

func Count(expr Expr) Expr {
	return Constant(values.Int(1))
}
