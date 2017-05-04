package expr

import (
	"fmt"
	"math"
	"time"

	"github.com/getlantern/goexpr"
)

// AVG creates an Expr that obtains its value as the arithmetic mean over the
// given value.
func AVG(val interface{}) Expr {
	return WAVG(val, CONST(1))
}

// WAVG creates an Expr that obtains its value as the weighted arithmetic mean
// over the given value weighted by the given weight.
func WAVG(val interface{}, weight interface{}) Expr {
	return &avg{exprFor(val), exprFor(weight)}
}

type avg struct {
	Value  Expr
	Weight Expr
}

func (e *avg) Validate() error {
	err := validateWrappedInAggregate(e.Value)
	if err != nil {
		return err
	}
	if e.Weight.EncodedWidth() > 0 {
		return fmt.Errorf("Weight expression %v must be a constant or directly derived from a field", e.Weight)
	}
	return nil
}

func (e *avg) EncodedWidth() int {
	return width64bits*2 + 1 + e.Value.EncodedWidth()
}

func (e *avg) Shift() time.Duration {
	a := e.Value.Shift()
	b := e.Weight.Shift()
	if a > b {
		return a
	}
	return b
}

func (e *avg) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	count, total, _, remain := e.load(b)
	remain, value, updated := e.Value.Update(remain, params, metadata)
	remain, weight, _ := e.Weight.Update(remain, params, metadata)
	if updated {
		count += weight
		total += value * weight
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

func (e *avg) SubMergers(subs []Expr) []SubMerge {
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

func (e *avg) subMerge(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
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

func (e *avg) IsConstant() bool {
	return e.Value.IsConstant()
}

func (e *avg) String() string {
	return fmt.Sprintf("AVG(%v)", e.Value)
}
