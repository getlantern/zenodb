package expr

import (
	"github.com/oxtoacart/tdb/values"
)

type averageValue struct {
	count float64
	total float64
}

func (a *averageValue) Val() float64 {
	return a.total / a.count
}

func (a *averageValue) Plus(addend values.Value) values.Value {
	b := addend.(*averageValue)
	a.count += b.count
	a.total += b.total
	return a
}

func Avg(expr Expr) Expr {
	return func(fields map[string]values.Value) values.Value {
		return &averageValue{1, expr(fields).Val()}
	}
}
