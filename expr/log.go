package expr

import (
	"fmt"
	"math"
	"time"

	"github.com/getlantern/goexpr"
)

type logExpr struct {
	Name    string
	Wrapped Expr
	Width   int
}

func (e *logExpr) getWithLog(fn func(float64) float64, b []byte) (float64, bool, []byte) {
	val, found, remain := e.Wrapped.Get(b)
	if found {
		val = fn(val)
	}
	return val, found, remain
}

type lnExpr struct {
	logExpr
}

func (e *lnExpr) Get(b []byte) (float64, bool, []byte) {
	return e.getWithLog(math.Log, b)
}

type log2Expr struct {
	logExpr
}

func (e *log2Expr) Get(b []byte) (float64, bool, []byte) {
	return e.getWithLog(math.Log2, b)
}

type log10Expr struct {
	logExpr
}

func (e *log10Expr) Get(b []byte) (float64, bool, []byte) {
	return e.getWithLog(math.Log10, b)
}

// LN is the natural logarithm
func LN(wrapped interface{}) Expr {
	_wrapped := exprFor(wrapped)
	return &lnExpr{logExpr{"LN", _wrapped, _wrapped.EncodedWidth()}}
}

// LOG2 is the binary logarithm
func LOG2(wrapped interface{}) Expr {
	_wrapped := exprFor(wrapped)
	return &log2Expr{logExpr{"LOG2", _wrapped, _wrapped.EncodedWidth()}}
}

// LOG10 is the base 10 logarithm
func LOG10(wrapped interface{}) Expr {
	_wrapped := exprFor(wrapped)
	return &log10Expr{logExpr{"LOG10", _wrapped, _wrapped.EncodedWidth()}}
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

func (e *logExpr) IsConstant() bool {
	return e.Wrapped.IsConstant()
}

func (e *logExpr) String() string {
	return fmt.Sprintf("%v(%v)", e.Name, e.Wrapped)
}

//
// func (e *logExpr) DecodeMsgpack(dec *msgpack.Decoder) error {
// 	m := make(map[string]interface{})
// 	err := dec.Decode(&m)
// 	if err != nil {
// 		return err
// 	}
// 	e2 := binaryExprFor(m["Op"].(string), m["Left"].(Expr), m["Right"].(Expr))
// 	if e2 == nil {
// 		return fmt.Errorf("Unknown binary expression %v", m["Op"])
// 	}
// 	e.Op = e2.Op
// 	e.Left = e2.Left
// 	e.Right = e2.Right
// 	e.calc = e2.calc
// 	return nil
// }
