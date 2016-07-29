package expr

import (
	"fmt"
	"reflect"
	"strconv"
)

var (
	fieldType     = reflect.TypeOf((*field)(nil))
	constType     = reflect.TypeOf((*constant)(nil))
	aggregateType = reflect.TypeOf((*aggregate)(nil))
	avgType       = reflect.TypeOf((*avg)(nil))
	binaryType    = reflect.TypeOf((*binaryExpr)(nil))
)

type Params interface {
	Get(name string) (float64, bool)
}

// Map is an implementation of the Params interface using a map.
type Map map[string]float64

// Get implements the method from the Params interface
func (p Map) Get(name string) (float64, bool) {
	val, found := p[name]
	return val, found
}

type Expr interface {
	Validate() error

	DependsOn() []string

	// Note - encoding to bytes is only valid for aggregate accumulators
	EncodedWidth() int

	// Update updates the value in buf by applying the given params.
	Update(b []byte, params Params) (remain []byte, value float64, updated bool)

	// Merge merges x and y, writing the result to b. It returns the remaining
	// portions of x and y.
	Merge(b []byte, x []byte, y []byte) (remainB []byte, remainX []byte, remainY []byte)

	// Get gets the value in buf, returning the value, a boolean indicating
	// whether or not the value was actually set, and the remaining byte array
	// after consuming the underlying data.
	Get(b []byte) (value float64, ok bool, remain []byte)

	String() string
}

func exprFor(expr interface{}) Expr {
	switch e := expr.(type) {
	case Expr:
		return e
	case string:
		v, err := strconv.ParseFloat(e, 64)
		if err == nil {
			return CONST(v)
		}
		return FIELD(e)
	case int:
		return CONST(float64(e))
	case int64:
		return CONST(float64(e))
	case int32:
		return CONST(float64(e))
	case int16:
		return CONST(float64(e))
	case byte:
		return CONST(float64(e))
	case float32:
		return CONST(float64(e))
	case float64:
		return CONST(e)
	default:
		panic(fmt.Sprintf("Got a %v, please specify an Expr, string, float64 or integer", reflect.TypeOf(expr)))
	}
}
