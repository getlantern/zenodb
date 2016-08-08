package expr

import (
	"github.com/Knetic/govaluate"
)

type ifExpr struct {
	cond         *govaluate.EvaluableExpression
	wrapped      Expr
	encodedWidth int
}

func IF(cond string, wrapped interface{}) (Expr, error) {
	_wrapped := exprFor(wrapped)
	_cond, err := govaluate.NewEvaluableExpression(cond)
	if err != nil {
		return nil, err
	}
	return &ifExpr{_cond, _wrapped, _wrapped.EncodedWidth()}, nil
}

func (e *ifExpr) Validate() error {
	return e.wrapped.Validate()
}

func (e *ifExpr) EncodedWidth() int {
	return e.encodedWidth
}

func (e *ifExpr) Update(b []byte, params Params, metadata govaluate.Parameters) ([]byte, float64, bool) {
	if e.include(metadata) {
		return e.wrapped.Update(b, params, metadata)
	}
	value, _, remain := e.wrapped.Get(b)
	return remain, value, false
}

func (e *ifExpr) Merge(b []byte, x []byte, y []byte, metadata govaluate.Parameters) ([]byte, []byte, []byte) {
	if e.include(metadata) {
		return e.wrapped.Merge(b, x, y, metadata)
	}
	return b[e.encodedWidth:], x[e.encodedWidth:], y[e.encodedWidth:]
}

func (e *ifExpr) SubMergers(subs []Expr) []SubMerge {
	sms := e.wrapped.SubMergers(subs)
	for i, sm := range sms {
		sms[i] = e.condSubMerger(sm)
	}
	return sms
}

func (e *ifExpr) condSubMerger(wrapped SubMerge) SubMerge {
	if wrapped == nil {
		return nil
	}
	return func(data []byte, other []byte, metadata govaluate.Parameters) {
		if e.include(metadata) {
			wrapped(data, other, metadata)
		}
	}
}

func (e *ifExpr) subMerge(data []byte, other []byte, metadata govaluate.Parameters) {
	e.wrapped.Merge(data, data, other, metadata)
}

func (e *ifExpr) include(metadata govaluate.Parameters) bool {
	if metadata == nil || e.cond == nil {
		return true
	}
	result, err := e.cond.Eval(metadata)
	return err == nil && result.(bool)
}

func (e *ifExpr) Get(b []byte) (float64, bool, []byte) {
	return e.wrapped.Get(b)
}

func (e *ifExpr) String() string {
	return e.wrapped.String()
}
