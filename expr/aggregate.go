package expr

import (
	"fmt"
	"math"
	"reflect"
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

func (a *aggregateAccumulator) EncodedWidth() int {
	return width64bits + a.wrapped.EncodedWidth()
}

func (a *aggregateAccumulator) Encode(b []byte) int {
	binaryEncoding.PutUint64(b, math.Float64bits(a.value))
	return width64bits + a.wrapped.Encode(b[width64bits:])
}

func (a *aggregateAccumulator) InitFrom(b []byte) []byte {
	a.value = math.Float64frombits(binaryEncoding.Uint64(b))
	return a.wrapped.InitFrom(b[width64bits:])
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

func (e *agg) Validate() error {
	return validateWrappedInAggregate(e.wrapped)
}

func validateWrappedInAggregate(wrapped Expr) error {
	if wrapped == nil {
		return fmt.Errorf("Aggregate cannot wrap nil expression")
	}
	typeOfWrapped := reflect.TypeOf(wrapped)
	if typeOfWrapped != fieldType && typeOfWrapped != constType {
		return fmt.Errorf("Aggregate can only wrap field and constant expressions, not %v", typeOfWrapped)
	}
	return wrapped.Validate()
}

func (e *agg) DependsOn() []string {
	return e.wrapped.DependsOn()
}

func (e *agg) String() string {
	return fmt.Sprintf("%v(%v)", e.name, e.wrapped)
}
