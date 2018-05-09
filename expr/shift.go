package expr

import (
	"fmt"
	"time"

	"github.com/getlantern/goexpr"
)

type shift struct {
	Wrapped Expr
	Offset  time.Duration
	Width   int
}

func SHIFT(wrapped interface{}, offset time.Duration) Expr {
	_wrapped := exprFor(wrapped)
	return &shift{_wrapped, offset, _wrapped.EncodedWidth()}
}

func (e *shift) Validate() error {
	return e.Wrapped.Validate()
}

func (e *shift) EncodedWidth() int {
	return e.Width
}

func (e *shift) Shift() time.Duration {
	return e.Offset + e.Wrapped.Shift()
}

func (e *shift) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	return e.Wrapped.Update(b, params, metadata)
}

func (e *shift) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return e.Wrapped.Merge(b, x, y)
}

func (e *shift) SubMergers(subs []Expr) []SubMerge {
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
		sms[i] = e.shiftedSubMerger(sm, subs[i].EncodedWidth())
	}
	return sms
}

func (e *shift) shiftedSubMerger(wrapped SubMerge, subWidth int) SubMerge {
	if wrapped == nil {
		return nil
	}
	return func(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
		n := -1 * int(e.Offset/otherRes) * subWidth
		if n >= 0 && n < len(other) {
			wrapped(data, other[n:], otherRes, metadata)
		}
	}
}

func (e *shift) subMerge(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
	e.Wrapped.Merge(data, data, other)
}

func (e *shift) Get(b []byte) (float64, bool, []byte) {
	return e.Wrapped.Get(b)
}

func (e *shift) IsConstant() bool {
	return e.Wrapped.IsConstant()
}

func (e *shift) DeAggregate() Expr {
	return SHIFT(e.Wrapped.DeAggregate(), e.Offset)
}

func (e *shift) String() string {
	return fmt.Sprintf("SHIFT(%v, %v)", e.Wrapped, e.Offset)
}
