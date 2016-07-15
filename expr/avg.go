package expr

import (
	"fmt"
	"math"
)

// AVG creates an Expr that obtains its value by averaging the values of the
// given expression or field.
func AVG(expr interface{}) Expr {
	return &avg{exprFor(expr)}
}

type avgAccumulator struct {
	wrapped Accumulator
	count   float64
	total   float64
}

func (a *avgAccumulator) Update(params Params) {
	a.wrapped.Update(params)
	a.count++
	a.total += a.wrapped.Get()
}

func (a *avgAccumulator) Get() float64 {
	if a.count == 0 {
		return 0
	}
	return a.total / a.count
}

func (a *avgAccumulator) EncodedWidth() int {
	return width64bits*2 + a.wrapped.EncodedWidth()
}

func (a *avgAccumulator) Encode(b []byte) int {
	binaryEncoding.PutUint64(b, math.Float64bits(a.count))
	binaryEncoding.PutUint64(b[width64bits:], math.Float64bits(a.total))
	return width64bits*2 + a.wrapped.Encode(b[width64bits*2:])
}

func (a *avgAccumulator) InitFrom(b []byte) []byte {
	a.count = math.Float64frombits(binaryEncoding.Uint64(b))
	a.total = math.Float64frombits(binaryEncoding.Uint64(b[width64bits:]))
	return a.wrapped.InitFrom(b[width64bits*2:])
}

type avg struct {
	wrapped Expr
}

func (e *avg) Accumulator() Accumulator {
	return &avgAccumulator{wrapped: e.wrapped.Accumulator()}
}

func (e *avg) DependsOn() []string {
	return e.wrapped.DependsOn()
}

func (e *avg) String() string {
	return fmt.Sprintf("AVG(%v)", e.wrapped)
}
