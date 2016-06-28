package expr

import (
	"github.com/oxtoacart/tdb/values"
)

type minvalue float64

type maxvalue float64

func (v minvalue) Val() float64 {
	return float64(v)
}

func (v maxvalue) Val() float64 {
	return float64(v)
}

func (v minvalue) Plus(addend values.Value) values.Value {
	if addend.Val() < v.Val() {
		return addend
	}
	return v
}

func (v maxvalue) Plus(addend values.Value) values.Value {
	if addend.Val() > v.Val() {
		return addend
	}
	return v
}

func Min(expr Expr) Expr {
	return func(fields map[string]values.Value) values.Value {
		return minvalue(expr(fields).Val())
	}
}

func Max(expr Expr) Expr {
	return func(fields map[string]values.Value) values.Value {
		return maxvalue(expr(fields).Val())
	}
}
