package expr

import (
	"fmt"
	"time"

	"github.com/getlantern/goexpr"
)

// CONST returns an Accumulator that always has a constant value.
func CONST(value float64) Expr {
	return &constant{value}
}

type constant struct {
	Value float64
}

func (e *constant) Validate() error {
	return nil
}

func (e *constant) EncodedWidth() int {
	return 0
}

func (e *constant) Shift() time.Duration {
	return 0
}

func (e *constant) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	return b, e.Value, false
}

func (e *constant) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return b, x, y
}

func (e *constant) SubMergers(subs []Expr) []SubMerge {
	return make([]SubMerge, len(subs))
}

func (e *constant) Get(b []byte) (float64, bool, []byte) {
	return e.Value, true, b
}

func (e *constant) IsConstant() bool {
	return true
}

func (e *constant) DeAggregate() Expr {
	return e
}

func (e *constant) String() string {
	return fmt.Sprintf("%f", e.Value)
}
