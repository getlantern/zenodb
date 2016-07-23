package expr

import (
	"fmt"
	"math"
	"reflect"
)

type updateFN func(current float64, next float64) float64

type aggregate struct {
	name    string
	wrapped Expr
	update  updateFN
}

func (e *aggregate) Validate() error {
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

func (e *aggregate) DependsOn() []string {
	return e.wrapped.DependsOn()
}

func (e *aggregate) EncodedWidth() int {
	return width64bits + e.wrapped.EncodedWidth()
}

func (e *aggregate) Update(b []byte, params Params) ([]byte, float64, bool) {
	value, more := e.load(b)
	remain, wrappedValue, updated := e.wrapped.Update(more, params)
	if updated {
		value = e.update(value, wrappedValue)
		e.save(b, value)
	}
	return remain, value, updated
}

func (e *aggregate) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	valueX, remainX := e.load(x)
	valueY, remainY := e.load(y)
	valueX = e.update(valueX, valueY)
	b = e.save(b, valueX)
	return b, remainX, remainY
}

func (e *aggregate) Get(b []byte) (float64, []byte) {
	return e.load(b)
}

func (e *aggregate) load(b []byte) (float64, []byte) {
	value := math.Float64frombits(binaryEncoding.Uint64(b))
	remain := b[width64bits:]
	return value, remain
}

func (e *aggregate) save(b []byte, value float64) []byte {
	binaryEncoding.PutUint64(b, math.Float64bits(value))
	return b[width64bits:]
}

func (e *aggregate) String() string {
	return fmt.Sprintf("%v(%v)", e.name, e.wrapped)
}
