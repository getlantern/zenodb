package expr

import (
	"sort"
)

type binaryAccumulator struct {
	left  Accumulator
	right Accumulator
}

func (a *binaryAccumulator) Update(params Params) {
	a.left.Update(params)
	a.right.Update(params)
}

type binaryExpr struct {
	left  Expr
	right Expr
}

func (e *binaryExpr) DependsOn() []string {
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
