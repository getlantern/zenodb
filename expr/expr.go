package expr

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"

	"github.com/getlantern/goexpr"
)

const (
	width64bits = 8
)

var (
	binaryEncoding = binary.BigEndian

	fieldType     = reflect.TypeOf((*field)(nil))
	constType     = reflect.TypeOf((*constant)(nil))
	aggregateType = reflect.TypeOf((*aggregate)(nil))
	ifType        = reflect.TypeOf((*ifExpr)(nil))
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

// SubMerge is a function that merges other into data for a given Expr,
// potentially taking into account the supplied metadata.
type SubMerge func(data []byte, other []byte, metadata goexpr.Params)

type Expr interface {
	Validate() error

	// Note - encoding to bytes is only valid for aggregate accumulators
	EncodedWidth() int

	// Update updates the value in buf by applying the given params. Metadata
	// provides additional metadata that can be used in evaluating how to apply
	// the update.
	Update(b []byte, params Params, metadata goexpr.Params) (remain []byte, value float64, updated bool)

	// Merge merges x and y, writing the result to b. It returns the remaining
	// portions of x and y.
	Merge(b []byte, x []byte, y []byte, metadata goexpr.Params) (remainB []byte, remainX []byte, remainY []byte)

	// SubMergers returns a list of function that merge values of the given
	// subexpressions into this Expr. The list is the same length as the number of
	// sub expressions. For any subexpression that is not represented in our
	// Expression, the corresonding function in the list is nil.
	SubMergers(subs []Expr) []SubMerge

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
