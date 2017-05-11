package expr

import (
	"fmt"
	"time"

	"github.com/getlantern/goexpr"
)

type ifExpr struct {
	Cond    goexpr.Expr
	Wrapped Expr
	Width   int
}

func IF(cond goexpr.Expr, wrapped interface{}) Expr {
	_wrapped := exprFor(wrapped)
	return &ifExpr{cond, _wrapped, _wrapped.EncodedWidth()}
}

func (e *ifExpr) Validate() error {
	return e.Wrapped.Validate()
}

func (e *ifExpr) EncodedWidth() int {
	return e.Width
}

func (e *ifExpr) Shift() time.Duration {
	return e.Wrapped.Shift()
}

func (e *ifExpr) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	if e.include(metadata) {
		return e.Wrapped.Update(b, params, metadata)
	}
	value, _, remain := e.Wrapped.Get(b)
	return remain, value, false
}

func (e *ifExpr) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return e.Wrapped.Merge(b, x, y)
}

func (e *ifExpr) SubMergers(subs []Expr) []SubMerge {
	sms := make([]SubMerge, len(subs))
	matched := false
	for i, sub := range subs {
		if e.String() == sub.String() {
			sms[i] = e.subMerge
			matched = true
		}
	}
	if matched {
		// We have an exact match, use that
		return sms
	}

	sms = e.Wrapped.SubMergers(subs)
	for i, sm := range sms {
		sms[i] = e.condSubMerger(sm)
	}
	return sms
}

func (e *ifExpr) condSubMerger(wrapped SubMerge) SubMerge {
	if wrapped == nil {
		return nil
	}
	return func(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
		if e.include(metadata) {
			wrapped(data, other, otherRes, metadata)
		}
	}
}

func (e *ifExpr) subMerge(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
	e.Wrapped.Merge(data, data, other)
}

func (e *ifExpr) include(metadata goexpr.Params) bool {
	if metadata == nil || e.Cond == nil {
		return true
	}
	val := e.Cond.Eval(metadata)
	return val != nil && val.(bool)
}

func (e *ifExpr) Get(b []byte) (float64, bool, []byte) {
	return e.Wrapped.Get(b)
}

func (e *ifExpr) IsConstant() bool {
	return e.Wrapped.IsConstant()
}

func (e *ifExpr) String() string {
	return fmt.Sprintf("IF(%v, %v)", e.Cond, e.Wrapped)
}
