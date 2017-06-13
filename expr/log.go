package expr

import (
	"fmt"
	"math"
	"time"

	"github.com/getlantern/goexpr"
)

type logExpr struct {
	Name    string
	LogFN   func(float64) float64
	Wrapped Expr
	Width   int
}

// LN is the natural logarithm
func LN(wrapped interface{}) Expr {
	_wrapped := exprFor(wrapped)
	return &logExpr{"LN", math.Log, _wrapped, _wrapped.EncodedWidth()}
}

// LOG2 is the binary logarithm
func LOG2(wrapped interface{}) Expr {
	_wrapped := exprFor(wrapped)
	return &logExpr{"LOG2", math.Log2, _wrapped, _wrapped.EncodedWidth()}
}

// LOG10 is the base 10 logarithm
func LOG10(wrapped interface{}) Expr {
	_wrapped := exprFor(wrapped)
	return &logExpr{"LOG10", math.Log10, _wrapped, _wrapped.EncodedWidth()}
}

func (e *logExpr) Validate() error {
	return e.Wrapped.Validate()
}

func (e *logExpr) EncodedWidth() int {
	return e.Width
}

func (e *logExpr) Shift() time.Duration {
	return e.Wrapped.Shift()
}

func (e *logExpr) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	return e.Wrapped.Update(b, params, metadata)
}

func (e *logExpr) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return e.Wrapped.Merge(b, x, y)
}

func (e *logExpr) SubMergers(subs []Expr) []SubMerge {
	return e.Wrapped.SubMergers(subs)
}

func (e *logExpr) Get(b []byte) (float64, bool, []byte) {
	return e.Wrapped.Get(b)
}

func (e *logExpr) IsConstant() bool {
	return e.Wrapped.IsConstant()
}

func (e *logExpr) String() string {
	return fmt.Sprintf("%v(%v)", e.Name, e.Wrapped)
}
