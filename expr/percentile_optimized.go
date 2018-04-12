package expr

import (
	"fmt"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/msgpack"
)

// newPtileOptimized creates a percentile that wraps an existing percentile and
// reuses its storage, simply using a different percentile value.
func newPtileOptimized(wrapped *ptile, percentile Expr) Expr {
	return &ptileOptimized{Wrapped: wrapped, wrapped: wrapped, Percentile: percentile}
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
	return e.wrapped.EncodedWidth()
}

func (e *ptileOptimized) Shift() time.Duration {
	return e.wrapped.Shift()
}

func (e *ptileOptimized) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	return e.wrapped.doUpdate(b, params, metadata, e.Percentile)
}

func (e *ptileOptimized) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return e.wrapped.Merge(b, x, y)
}

func (e *ptileOptimized) SubMergers(subs []Expr) []SubMerge {
	return e.wrapped.SubMergers(subs)
}

func (e *ptileOptimized) Get(b []byte) (float64, bool, []byte) {
	return e.wrapped.doGet(b, e.Percentile)
}

func (e *ptileOptimized) IsConstant() bool {
	return false
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
