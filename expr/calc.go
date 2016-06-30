package expr

import (
	"sort"
)

type calcFN func(left float64, right float64) float64

// calc creates an Expr that obtains its value by applying the given calcFN
func calc(left interface{}, right interface{}, calc calcFN) Expr {
	return &calculator{exprFor(left), exprFor(right), calc}
}

type calcAccumulator struct {
	left  Accumulator
	right Accumulator
	calc  calcFN
}

func (a *calcAccumulator) Update(params Params) {
	a.left.Update(params)
	a.right.Update(params)
}

func (a *calcAccumulator) Get() float64 {
	return a.calc(a.left.Get(), a.right.Get())
}

type calculator struct {
	left  Expr
	right Expr
	calc  calcFN
}

func (e *calculator) Accumulator() Accumulator {
	return &calcAccumulator{e.left.Accumulator(), e.right.Accumulator(), e.calc}
}

func (e *calculator) DependsOn() []string {
	m := make(map[string]bool, 0)
	for _, param := range e.left.DependsOn() {
		m[param] = true
	}
	for _, param := range e.right.DependsOn() {
		m[param] = true
	}
	result := make([]string, 0, len(m))
	for param := range m {
		result = append(result, param)
	}
	sort.Strings(result)
	return result
}
