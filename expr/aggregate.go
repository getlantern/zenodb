package expr

import (
	"encoding/binary"
	"fmt"
	"math"
)

type updateFN func(current float64, next float64) float64

// aggregate creates an Expr that obtains its value by doing aggregation
func aggregate(name string, expr interface{}, defaultValue float64, update updateFN) Expr {
	return &agg{name, exprFor(expr), defaultValue, update}
}

type aggregateAccumulator struct {
	wrapped      Accumulator
	update       updateFN
	defaultValue float64
	value        float64
}

func (a *aggregateAccumulator) Update(params Params) {
	a.wrapped.Update(params)
	a.value = a.update(a.value, a.wrapped.Get())
}

func (a *aggregateAccumulator) Get() float64 {
	if a.value == a.defaultValue {
		return 0
	}
	return a.value
}

func (a *aggregateAccumulator) Bytes() []byte {
	b := make([]byte, size64bits)
	binary.BigEndian.PutUint64(b, math.Float64bits(a.value))
	return append(b, a.wrapped.Bytes()...)
}

func (a *aggregateAccumulator) InitFrom(b []byte) []byte {
	a.value = math.Float64frombits(binary.BigEndian.Uint64(b))
	return a.wrapped.InitFrom(b[size64bits:])
}

type agg struct {
	name         string
	wrapped      Expr
	defaultValue float64
	update       updateFN
}

func (e *agg) Accumulator() Accumulator {
	return &aggregateAccumulator{
		wrapped:      e.wrapped.Accumulator(),
		update:       e.update,
		defaultValue: e.defaultValue,
		value:        e.defaultValue,
	}
}

func (e *agg) DependsOn() []string {
	return e.wrapped.DependsOn()
}

func (e *agg) String() string {
	return fmt.Sprintf("%v(%v)", e.name, e.wrapped)
}
