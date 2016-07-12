package expr

import (
	"fmt"
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

func (a *binaryAccumulator) Bytes() []byte {
	return append(a.left.Bytes(), a.right.Bytes()...)
}

func (a *binaryAccumulator) InitFrom(b []byte) []byte {
	return a.right.InitFrom(a.left.InitFrom(b))
}

type binaryExpr struct {
	op    string
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

func (e *binaryExpr) String() string {
	return fmt.Sprintf("%v %v %v", e.left, e.op, e.right)
}
