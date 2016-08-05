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
	return width64bits*2 + 1 + e.wrapped.EncodedWidth()
}

func (e *avg) Update(b []byte, params Params) ([]byte, float64, bool) {
	count, total, _, more := e.load(b)
	remain, wrappedValue, updated := e.wrapped.Update(more, params)
	if updated {
		count++
		total += wrappedValue
		e.save(b, count, total)
	}
	return remain, e.calc(count, total), updated
}

func (e *avg) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	countX, totalX, xWasSet, remainX := e.load(x)
	countY, totalY, yWasSet, remainY := e.load(y)
	if !xWasSet {
		if yWasSet {
			// Use valueY
			b = e.save(b, countY, totalY)
		} else {
			// Nothing to save, just advance
			b = b[width64bits*2+1:]
		}
	} else {
		if yWasSet {
			countX += countY
			totalX += totalY
		}
		b = e.save(b, countX, totalX)
	}
	return b, remainX, remainY
}

func (e *avg) SubMerger(sub Expr) SubMerge {
	if sub.String() == e.String() {
		return e.subMerge
	}
	return nil
}

func (e *avg) subMerge(data []byte, other []byte) {
	e.Merge(data, data, other)
}

func (e *avg) Get(b []byte) (float64, bool, []byte) {
	count, total, wasSet, remain := e.load(b)
	if !wasSet {
		return 0, wasSet, remain
	}
	return e.calc(count, total), wasSet, remain
}

func (e *avg) calc(count float64, total float64) float64 {
	if count == 0 {
		return 0
	}
	return total / count
}

func (e *avg) load(b []byte) (float64, float64, bool, []byte) {
	remain := b[width64bits*2+1:]
	wasSet := b[0] == 1
	count := float64(0)
	total := float64(0)
	if wasSet {
		count = math.Float64frombits(binaryEncoding.Uint64(b[1:]))
		total = math.Float64frombits(binaryEncoding.Uint64(b[width64bits+1:]))
	}
	return count, total, wasSet, remain
}

func (e *avg) save(b []byte, count float64, total float64) []byte {
	b[0] = 1
	binaryEncoding.PutUint64(b[1:], math.Float64bits(count))
	binaryEncoding.PutUint64(b[width64bits+1:], math.Float64bits(total))
	return b[width64bits*2+1:]
}

func (e *avg) String() string {
	return fmt.Sprintf("AVG(%v)", e.wrapped)
}
