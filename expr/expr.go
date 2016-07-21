package expr

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
)

const (
	width64bits = 8
)

var (
	binaryEncoding = binary.LittleEndian

	fieldType     = reflect.TypeOf((*field)(nil))
	constType     = reflect.TypeOf((*constant)(nil))
	aggregateType = reflect.TypeOf((*agg)(nil))
	avgType       = reflect.TypeOf((*avg)(nil))
	calcType      = reflect.TypeOf((*calculator)(nil))
	condType      = reflect.TypeOf((*conditional)(nil))
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

type Accumulator interface {
	Update(params Params) bool

	Get() float64

	// Note - encoding to bytes is only valid for aggregate accumulators
	EncodedWidth() int

	Encode(b []byte) int

	InitFrom(b []byte) []byte
}

func Encoded(accum Accumulator) []byte {
	b := make([]byte, accum.EncodedWidth())
	accum.Encode(b)
	return b
}

type Expr interface {
	Accumulator() Accumulator

	Validate() error

	DependsOn() []string

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
