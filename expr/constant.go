package expr

import (
	"fmt"
)

// CONST returns an Accumulator that always has a constant value.
func CONST(value float64) Expr {
	return &constant{value}
}

type constant struct {
	value float64
}

func (e *constant) Validate() error {
	return nil
}

func (e *constant) DependsOn() []string {
	return []string{}
}

func (e *constant) EncodedWidth() int {
	return 0
}

func (e *constant) Update(b []byte, params Params) ([]byte, float64, bool) {
	return b, 0, false
}

func (e *constant) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return b, x, y
}

func (e *constant) Get(b []byte) (float64, bool, []byte) {
	return e.value, true, b
}

func (e *constant) String() string {
	return fmt.Sprintf("%f", e.value)
}
