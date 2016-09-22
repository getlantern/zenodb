package expr

import (
	"github.com/getlantern/goexpr"
)

// BOUNDED bounds the given expression to min <= val <= max. Any values that
// fall outside of the bounds will appear as unset.
func BOUNDED(expr interface{}, min float64, max float64) Expr {
	wrapped := exprFor(expr)
	return &bounded{
		wrapped: wrapped,
		min:     min,
		max:     max,
	}
}

type bounded struct {
	wrapped Expr
	min     float64
	max     float64
}

func (e *bounded) Validate() error {
	return e.wrapped.Validate()
}

func (e *bounded) EncodedWidth() int {
	return e.wrapped.EncodedWidth()
}

func (e *bounded) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	remain, value, updated := e.wrapped.Update(b, params, metadata)
	if !e.test(value) {
		updated = false
		value = 0
	}
	return remain, value, updated
}

func (e *bounded) test(val float64) bool {
	return val >= e.min && val <= e.max
}

func (e *bounded) Merge(b []byte, x []byte, y []byte, metadata goexpr.Params) ([]byte, []byte, []byte) {
	return e.wrapped.Merge(b, x, y, metadata)
}

func (e *bounded) SubMergers(subs []Expr) []SubMerge {
	return e.wrapped.SubMergers(subs)
}

func (e *bounded) Get(b []byte) (float64, bool, []byte) {
	val, wasSet, remain := e.wrapped.Get(b)
	if !wasSet || !e.test(val) {
		return 0, false, remain
	}
	return val, wasSet, remain
}

func (e *bounded) String() string {
	return e.wrapped.String()
}
