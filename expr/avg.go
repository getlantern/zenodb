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

type avg struct {
	wrapped Expr
}

func (e *avg) DependsOn() []string {
	return e.wrapped.DependsOn()
}

func (e *avg) Validate() error {
	return validateWrappedInAggregate(e.wrapped)
}

func (e *avg) EncodedWidth() int {
	return width64bits*2 + e.wrapped.EncodedWidth()
}

func (e *avg) Update(b []byte, params Params) ([]byte, float64, bool) {
	count, total, more := e.load(b)
	remain, wrappedValue, updated := e.wrapped.Update(more, params)
	if updated {
		count++
		total += wrappedValue
		e.save(b, count, total)
	}
	return remain, e.calc(count, total), updated
}

func (e *avg) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	countX, totalX, remainX := e.load(x)
	countY, totalY, remainY := e.load(y)
	countX += countY
	totalX += totalY
	b = e.save(b, countX, totalX)
	return b, remainX, remainY
}

func (e *avg) Get(b []byte) (float64, []byte) {
	count, total, remain := e.load(b)
	return e.calc(count, total), remain
}

func (e *avg) calc(count float64, total float64) float64 {
	if count == 0 {
		return 0
	}
	return total / count
}

func (e *avg) load(b []byte) (float64, float64, []byte) {
	count := math.Float64frombits(binaryEncoding.Uint64(b))
	total := math.Float64frombits(binaryEncoding.Uint64(b[width64bits:]))
	return count, total, b[width64bits*2:]
}

func (e *avg) save(b []byte, count float64, total float64) []byte {
	binaryEncoding.PutUint64(b, math.Float64bits(count))
	binaryEncoding.PutUint64(b[width64bits:], math.Float64bits(total))
	return b[width64bits*2:]
}

func (e *avg) String() string {
	return fmt.Sprintf("AVG(%v)", e.wrapped)
}
