package expr

import (
	"fmt"
	"math"
	"reflect"

	"github.com/getlantern/goexpr"
)

type updateFN func(wasSet bool, current float64, next float64) float64

type aggregate struct {
	name    string
	wrapped Expr
	_update updateFN
	_merge  updateFN
}

func (e *aggregate) Validate() error {
	return validateWrappedInAggregate(e.wrapped)
}

func validateWrappedInAggregate(wrapped Expr) error {
	if wrapped == nil {
		return fmt.Errorf("Aggregate cannot wrap nil expression")
	}
	typeOfWrapped := reflect.TypeOf(wrapped)
	if typeOfWrapped != fieldType && typeOfWrapped != constType && typeOfWrapped != boundedType {
		return fmt.Errorf("Aggregate can only wrap field and constant expressions, not %v", typeOfWrapped)
	}
	return wrapped.Validate()
}

func (e *aggregate) EncodedWidth() int {
	return 1 + width64bits + e.wrapped.EncodedWidth()
}

func (e *aggregate) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	value, wasSet, more := e.load(b)
	remain, wrappedValue, updated := e.wrapped.Update(more, params, metadata)
	if updated {
		value = e._update(wasSet, value, wrappedValue)
		e.save(b, value)
	}
	return remain, value, updated
}

func (e *aggregate) Merge(b []byte, x []byte, y []byte, metadata goexpr.Params) ([]byte, []byte, []byte) {
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
			valueX = e._merge(true, valueX, valueY)
		}
		b = e.save(b, valueX)
	}
	return b, remainX, remainY
}

func (e *aggregate) SubMergers(subs []Expr) []SubMerge {
	result := make([]SubMerge, 0, len(subs))
	for _, sub := range subs {
		var sm SubMerge
		if e.String() == sub.String() {
			sm = e.subMerge
		}
		result = append(result, sm)
	}
	return result
}

func (e *aggregate) subMerge(data []byte, other []byte, metadata goexpr.Params) {
	e.Merge(data, data, other, metadata)
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
