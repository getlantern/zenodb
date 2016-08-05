package expr

import (
	"fmt"
	"math"
	"reflect"
)

type updateFN func(wasSet bool, current float64, next float64) float64

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
	return 1 + width64bits + e.wrapped.EncodedWidth()
}

func (e *aggregate) Update(b []byte, params Params) ([]byte, float64, bool) {
	value, wasSet, more := e.load(b)
	remain, wrappedValue, updated := e.wrapped.Update(more, params)
	if updated {
		value = e.update(wasSet, value, wrappedValue)
		e.save(b, value)
	}
	return remain, value, updated
}

func (e *aggregate) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	fmt.Printf("%d : %d\n", len(x), len(y))
	valueX, xWasSet, remainX := e.load(x)
	valueY, yWasSet, remainY := e.load(y)
	if !xWasSet {
		if yWasSet {
			// Use valueY
			b = e.save(b, valueY)
		} else {
			// Nothing to save, just advance
			b = b[width64bits+1:]
		}
	} else {
		if yWasSet {
			// Update valueX from valueY
			valueX = e.update(true, valueX, valueY)
		}
		b = e.save(b, valueX)
	}
	return b, remainX, remainY
}

func (e *aggregate) SubMerger(sub Expr) SubMerge {
	if sub.String() == e.String() {
		return e.subMerge
	}
	return nil
}

func (e *aggregate) subMerge(data []byte, other []byte) {
	e.Merge(data, data, other)
}

func (e *aggregate) Get(b []byte) (float64, bool, []byte) {
	return e.load(b)
}

func (e *aggregate) load(b []byte) (float64, bool, []byte) {
	remain := b[width64bits+1:]
	value := float64(0)
	wasSet := b[0] == 1
	if wasSet {
		value = math.Float64frombits(binaryEncoding.Uint64(b[1:]))
	}
	return value, wasSet, remain
}

func (e *aggregate) save(b []byte, value float64) []byte {
	b[0] = 1
	binaryEncoding.PutUint64(b[1:], math.Float64bits(value))
	return b[width64bits+1:]
}

func (e *aggregate) String() string {
	return fmt.Sprintf("%v(%v)", e.name, e.wrapped)
}
