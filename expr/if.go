package expr

import (
	"fmt"
	"github.com/getlantern/goexpr"
)

type ifExpr struct {
	cond         goexpr.Expr
	wrapped      Expr
	encodedWidth int
}

func IF(cond goexpr.Expr, wrapped interface{}) (Expr, error) {
	_wrapped := exprFor(wrapped)
	return &ifExpr{cond, _wrapped, _wrapped.EncodedWidth()}, nil
}

func (e *ifExpr) Validate() error {
	return e.wrapped.Validate()
}

func (e *ifExpr) EncodedWidth() int {
	return e.encodedWidth
}

func (e *ifExpr) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	if e.include(metadata) {
		return e.wrapped.Update(b, params, metadata)
	}
	value, _, remain := e.wrapped.Get(b)
	return remain, value, false
}

func (e *ifExpr) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return e.wrapped.Merge(b, x, y)
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

	sms = e.wrapped.SubMergers(subs)
	for i, sm := range sms {
		sms[i] = e.condSubMerger(sm)
	}
	return sms
}

func (e *ifExpr) condSubMerger(wrapped SubMerge) SubMerge {
	if wrapped == nil {
		return nil
	}
	return func(data []byte, other []byte, metadata goexpr.Params) {
		if e.include(metadata) {
			wrapped(data, other, metadata)
		}
	}
}

func (e *ifExpr) subMerge(data []byte, other []byte, metadata goexpr.Params) {
	e.wrapped.Merge(data, data, other)
}

func (e *ifExpr) include(metadata goexpr.Params) bool {
	if metadata == nil || e.cond == nil {
		return true
	}
	val := e.cond.Eval(metadata)
	return val != nil && val.(bool)
}

func (e *ifExpr) Get(b []byte) (float64, bool, []byte) {
	return e.wrapped.Get(b)
}

func (e *ifExpr) String() string {
	return fmt.Sprintf("IF(%v, %v)", e.cond, e.wrapped)
}
