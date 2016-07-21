package expr

import (
	"fmt"
)

type calcFN func(left float64, right float64) float64

// calc creates an Expr that obtains its value by applying the given calcFN
func calc(op string, left interface{}, right interface{}, calc calcFN) Expr {
	return &calculator{binaryExpr{op, exprFor(left), exprFor(right)}, calc}
}

type calcAccumulator struct {
	binaryAccumulator
	calc calcFN
}

func (a *calcAccumulator) Merge(other Accumulator) {
	o, ok := other.(*calcAccumulator)
	if !ok {
		panic(fmt.Sprintf("%v is not a calcAccumulator!", other))
	}
	a.doMerge(&o.binaryAccumulator)
}

func (a *calcAccumulator) Get() float64 {
	return a.calc(a.left.Get(), a.right.Get())
}

type calculator struct {
	binaryExpr
	calc calcFN
}

func (e *calculator) Accumulator() Accumulator {
	return &calcAccumulator{binaryAccumulator{e.left.Accumulator(), e.right.Accumulator()}, e.calc}
}
