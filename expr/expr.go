// Package expr provides a framework for Expressions that evaluate to floating
// point values and allow various functions that can aggregate data, perform
// calculations on that data, evaluate boolean expressions against that data
// and serialize the data to/from bytes for durable storage in the database.
package expr

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/msgpack"
)

const (
	width64bits = 8
)

var (
	binaryEncoding = binary.BigEndian

	fieldType               = reflect.TypeOf((*field)(nil))
	constType               = reflect.TypeOf((*constant)(nil))
	boundedType             = reflect.TypeOf((*bounded)(nil))
	aggregateType           = reflect.TypeOf((*aggregate)(nil))
	ifType                  = reflect.TypeOf((*ifExpr)(nil))
	avgType                 = reflect.TypeOf((*avg)(nil))
	binaryType              = reflect.TypeOf((*binaryExpr)(nil))
	shiftType               = reflect.TypeOf((*shift)(nil))
	unaryMathType           = reflect.TypeOf((*unaryMathExpr)(nil))
	percentileType          = reflect.TypeOf((*ptile)(nil))
	percentileOptimizedType = reflect.TypeOf((*ptileOptimized)(nil))
)

func init() {
	msgpack.RegisterExt(50, &field{})
	msgpack.RegisterExt(51, &constant{})
	msgpack.RegisterExt(52, &bounded{})
	msgpack.RegisterExt(53, &aggregate{})
	msgpack.RegisterExt(54, &ifExpr{})
	msgpack.RegisterExt(55, &avg{})
	msgpack.RegisterExt(56, &binaryExpr{})
	msgpack.RegisterExt(57, &shift{})
	msgpack.RegisterExt(58, &unaryMathExpr{})
	msgpack.RegisterExt(59, &ptile{})
	msgpack.RegisterExt(60, &ptileOptimized{})
}

// Params is an interface for data structures that can contain named values.
type Params interface {
	// Get returns the named value. Found should be false if nothing was found for
	// the given name.
	Get(name string) (val float64, found bool)
}

// Map is an implementation of the Params interface using a map.
type Map map[string]float64

// Get implements the method from the Params interface
func (p Map) Get(name string) (val float64, found bool) {
	val, found = p[name]
	return val, found
}

// FloatParams is an implementation of Params that always returns the same
// float64 value.
type FloatParams float64

func (p FloatParams) Get(name string) (val float64, found bool) {
	return float64(p), true
}

// SubMerge is a function that merges other into data for a given Expr,
// potentially taking into account the supplied metadata. otherRes is the amount
// of time represented by each period in other.
type SubMerge func(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params)

// An Expr is expression that stores its value in a byte array and that
// evaluates to a float64.
type Expr interface {
	// Validate makes sure that this expression is valid and returns an error if
	// it is not.
	Validate() error

	// EncodedWidth returns the number of bytes needed to represent the internal
	// state of this Expr.
	EncodedWidth() int

	// Shift returns the total cumulative shift in time, including
	// subexpressions.
	Shift() time.Duration

	// Update updates the value in buf by applying the given Params. Metadata
	// provides additional metadata that can be used in evaluating how to apply
	// the update.
	Update(b []byte, params Params, metadata goexpr.Params) (remain []byte, value float64, updated bool)

	// Merge merges x and y, writing the result to b. It returns the remaining
	// portions of x and y.
	Merge(b []byte, x []byte, y []byte) (remainB []byte, remainX []byte, remainY []byte)

	// SubMergers returns a list of functions that merge values of the given
	// subexpressions into this Expr. The list is the same length as the number of
	// sub expressions. For any subexpression that is not represented in our
	// Expression, the corresponding function in the list is nil.
	SubMergers(subs []Expr) []SubMerge

	// Get gets the value in buf, returning the value, a boolean indicating
	// whether or not the value was actually set, and the remaining byte array
	// after consuming the underlying data.
	Get(b []byte) (value float64, ok bool, remain []byte)

	// IsConstant indicates whether or not this is a constant expression
	IsConstant() bool

	// DeAggregate strips aggregates from this expression and returns the result
	DeAggregate() Expr

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
