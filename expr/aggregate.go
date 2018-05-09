package expr

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/msgpack"
)

var aggregates = make(map[string]func(wrapped interface{}) *aggregate)

func aggregateFor(name string, wrapped interface{}) *aggregate {
	ctor, found := aggregates[name]
	if !found {
		return nil
	}
	return ctor(wrapped)
}

func registerAggregate(name string, update updateFN, merge updateFN) {
	aggregates[name] = func(wrapped interface{}) *aggregate {
		return &aggregate{
			Name:    name,
			Wrapped: exprFor(wrapped),
			update:  update,
			merge:   merge,
		}
	}
}

type updateFN func(wasSet bool, current float64, next float64) float64

type aggregate struct {
	Name    string
	Wrapped Expr
	update  updateFN
	merge   updateFN
}

func (e *aggregate) Validate() error {
	return validateWrappedInAggregate(e.Wrapped)
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
	return 1 + width64bits + e.Wrapped.EncodedWidth()
}

func (e *aggregate) Shift() time.Duration {
	return e.Wrapped.Shift()
}

func (e *aggregate) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	value, wasSet, more := e.load(b)
	remain, wrappedValue, updated := e.Wrapped.Update(more, params, metadata)
	if updated {
		value = e.update(wasSet, value, wrappedValue)
		e.save(b, value)
	}
	return remain, value, updated
}

func (e *aggregate) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
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
			valueX = e.merge(true, valueX, valueY)
		}
		b = e.save(b, valueX)
	}
	return b, remainX, remainY
}

func (e *aggregate) SubMergers(subs []Expr) []SubMerge {
	result := make([]SubMerge, len(subs))
	for i, sub := range subs {
		if e.String() == sub.String() {
			result[i] = e.subMerge
		}
	}
	return result
}

func (e *aggregate) subMerge(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
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

func (e *aggregate) IsConstant() bool {
	return e.Wrapped.IsConstant()
}

func (e *aggregate) DeAggregate() Expr {
	return e.Wrapped.DeAggregate()
}

func (e *aggregate) String() string {
	return fmt.Sprintf("%v(%v)", e.Name, e.Wrapped)
}

func (e *aggregate) DecodeMsgpack(dec *msgpack.Decoder) error {
	m := make(map[string]interface{})
	err := dec.Decode(&m)
	if err != nil {
		return err
	}
	e2 := aggregateFor(m["Name"].(string), m["Wrapped"].(Expr))
	if e2 == nil {
		return fmt.Errorf("Unknown aggregate %v", m["Name"])
	}
	e.Name = e2.Name
	e.Wrapped = e2.Wrapped
	e.update = e2.update
	e.merge = e2.merge
	return nil
}
