package expr

import (
	"fmt"

	"github.com/getlantern/msgpack"
)

// PERCENTILEOPT returns an optimized PERCENTILE that wraps an existing
// PERCENTILE.
func PERCENTILEOPT(wrapped interface{}, percentile interface{}) Expr {
	var expr Expr
	switch t := wrapped.(type) {
	case *ptileOptimized:
		expr = &t.ptile
	default:
		expr = wrapped.(*ptile)
	}
	return &ptileOptimized{Wrapped: expr, ptile: *expr.(*ptile), Percentile: exprFor(percentile)}
}

type ptileOptimized struct {
	ptile
	Wrapped    Expr
	Percentile Expr
}

func (e *ptileOptimized) Get(b []byte) (float64, bool, []byte) {
	histo, wasSet, remain := e.ptile.load(b)
	percentile, _, remain := e.Percentile.Get(remain)
	if !wasSet {
		return 0, wasSet, remain
	}
	return e.ptile.calc(histo, percentile), wasSet, remain
}

func (e *ptileOptimized) String() string {
	return fmt.Sprintf("PERCENTILE(%v, %v)", e.Wrapped.String(), e.Percentile)
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
	e.ptile = *wrapped
	e.Percentile = percentile
	return nil
}
