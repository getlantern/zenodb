package expr

import (
	"fmt"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/msgpack"
)

// PERCENTILEOPT returns an optimized PERCENTILE that wraps an existing
// PERCENTILE and reuses its storage.
func PERCENTILEOPT(wrapped interface{}, percentile interface{}) Expr {
	var expr Expr
	switch t := wrapped.(type) {
	case *ptileOptimized:
		expr = t.wrapped
	default:
		expr = wrapped.(*ptile)
	}
	return &ptileOptimized{Wrapped: expr, wrapped: expr.(*ptile), Percentile: exprFor(percentile)}
}

type ptileOptimized struct {
	Wrapped    Expr
	wrapped    *ptile
	Percentile Expr
}

func (e *ptileOptimized) Validate() error {
	return e.wrapped.Validate()
}

func (e *ptileOptimized) EncodedWidth() int {
	return 0
}

func (e *ptileOptimized) Shift() time.Duration {
	return 0
}

func (e *ptileOptimized) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	return b, 0, false
}

func (e *ptileOptimized) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return b, x, y
}

func (e *ptileOptimized) SubMergers(subs []Expr) []SubMerge {
	return nil
}

func (e *ptileOptimized) Get(b []byte) (float64, bool, []byte) {
	histo, wasSet, remain := e.wrapped.load(b)
	percentile, _, remain := e.Percentile.Get(remain)
	if !wasSet {
		return 0, wasSet, remain
	}
	return e.wrapped.calc(histo, percentile), wasSet, remain
}

func (e *ptileOptimized) IsConstant() bool {
	return false
}

func (e *ptileOptimized) DeAggregate() Expr {
	return PERCENTILE(e.wrapped.DeAggregate(), e.Percentile.DeAggregate(), scaleFromInt(e.wrapped.Min, e.wrapped.Precision), scaleFromInt(e.wrapped.Max, e.wrapped.Precision), e.wrapped.Precision)
}

func (e *ptileOptimized) String() string {
	return fmt.Sprintf("PERCENTILE(%v, %v)", e.wrapped.String(), e.Percentile)
}

func (e *ptileOptimized) DecodeMsgpack(dec *msgpack.Decoder) error {
	m := make(map[string]interface{})
	err := dec.Decode(&m)
	if err != nil {
		return err
	}
	wrapped := m["Wrapped"].(*ptile)
	percentile := m["Percentile"].(Expr)
	e.Wrapped = wrapped
	e.wrapped = wrapped
	e.Percentile = percentile
	return nil
}
