package expr

import (
	"github.com/Knetic/govaluate"
)

type conditionable interface {
	validate() error

	encodedWidth() int

	get(b []byte) (float64, bool, []byte)

	update(b []byte, params Params, metadata govaluate.Parameters) ([]byte, float64, bool)

	merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte)

	string(cond string) string
}

type conditioned struct {
	wrapped      conditionable
	cond         *govaluate.EvaluableExpression
	encodedWidth int
}

func newConditioned(cond *govaluate.EvaluableExpression, wrapped conditionable) Expr {
	return &conditioned{wrapped, cond, wrapped.encodedWidth()}
}

func (e *conditioned) Validate() error {
	return e.wrapped.validate()
}

func (e *conditioned) EncodedWidth() int {
	return e.encodedWidth
}

func (e *conditioned) Update(b []byte, params Params, metadata govaluate.Parameters) ([]byte, float64, bool) {
	if e.include(metadata) {
		return e.wrapped.update(b, params, metadata)
	}
	value, _, remain := e.wrapped.get(b)
	return remain, value, false
}

func (e *conditioned) Merge(b []byte, x []byte, y []byte, metadata govaluate.Parameters) ([]byte, []byte, []byte) {
	if e.include(metadata) {
		return e.wrapped.merge(b, x, y)
	}
	return b[e.encodedWidth:], x[e.encodedWidth:], y[e.encodedWidth:]
}

func (e *conditioned) SubMergers(subs []Expr) []SubMerge {
	sms := e.subMergers(subs)
	for i, sm := range sms {
		sms[i] = e.condSubMerger(sm)
	}
	return sms
}

func (e *conditioned) subMergers(subs []Expr) []SubMerge {
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

func (e *conditioned) condSubMerger(wrapped SubMerge) SubMerge {
	if wrapped == nil {
		return nil
	}
	return func(data []byte, other []byte, metadata govaluate.Parameters) {
		if e.include(metadata) {
			wrapped(data, other, metadata)
		}
	}
}

func (e *conditioned) subMerge(data []byte, other []byte, metadata govaluate.Parameters) {
	e.wrapped.merge(data, data, other)
}

func (e *conditioned) include(metadata govaluate.Parameters) bool {
	if metadata == nil || e.cond == nil {
		return true
	}
	result, err := e.cond.Eval(metadata)
	return err == nil && result.(bool)
}

func (e *conditioned) Get(b []byte) (float64, bool, []byte) {
	return e.wrapped.get(b)
}

func (e *conditioned) String() string {
	if e.cond == nil {
		return e.wrapped.string("")
	}
	return e.wrapped.string(e.cond.String())
}
