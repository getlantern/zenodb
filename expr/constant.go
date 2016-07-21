package expr

import (
	"fmt"
)

// CONST returns an Accumulator that always has a constant value.
func CONST(value float64) Expr {
	return &constant{value}
}

type constantAccumulator struct {
	value float64
}

func (a *constantAccumulator) Update(params Params) bool {
	return false
}

func (a *constantAccumulator) Merge(other Accumulator) {
}

func (a *constantAccumulator) Get() float64 {
	return a.value
}

func (a *constantAccumulator) EncodedWidth() int {
	return 0
}

func (a *constantAccumulator) Encode(b []byte) int {
	return 0
}

func (a *constantAccumulator) InitFrom(b []byte) []byte {
	return b
}

type constant struct {
	value float64
}

func (e *constant) Accumulator() Accumulator {
	return &constantAccumulator{e.value}
}

func (e *constant) Validate() error {
	return nil
}

func (e *constant) DependsOn() []string {
	return []string{}
}

func (e *constant) String() string {
	return fmt.Sprintf("%f", e.value)
}
