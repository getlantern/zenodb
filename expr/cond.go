package expr

import (
	"fmt"
)

// Cond is a special kind of expression whose accumulators always return either
// 1 or 0 for true/false.
type Cond interface {
	Expr
}

type compareFN func(left float64, right float64) bool

// cond creates a Cond that performs its comparison using the given compareFN
func cond(cond string, left interface{}, right interface{}, compare compareFN) Expr {
	return &conditional{binaryExpr{cond, exprFor(left), exprFor(right)}, compare}
}

type condAccumulator struct {
	binaryAccumulator
	compare compareFN
}

func (a *condAccumulator) Merge(other Accumulator) {
	o, ok := other.(*condAccumulator)
	if !ok {
		panic(fmt.Sprintf("%v is not a condAccumulator!", other))
	}
	a.doMerge(&o.binaryAccumulator)
}

func (a *condAccumulator) Get() float64 {
	if a.compare(a.left.Get(), a.right.Get()) {
		return 1
	}
	return 0
}

type conditional struct {
	binaryExpr
	compare compareFN
}

func (e *conditional) Accumulator() Accumulator {
	return &condAccumulator{binaryAccumulator{e.left.Accumulator(), e.right.Accumulator()}, e.compare}
}
