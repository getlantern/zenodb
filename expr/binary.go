package expr

import (
	"fmt"
	"reflect"
	"sort"
)

type calcFN func(left float64, right float64) float64

type binaryExpr struct {
	op    string
	left  Expr
	right Expr
	calc  calcFN
}

func (e *binaryExpr) Validate() error {
	err := validateWrappedInBinary(e.left)
	if err == nil {
		err = validateWrappedInBinary(e.right)
	}
	return err
}

func validateWrappedInBinary(wrapped Expr) error {
	if wrapped == nil {
		return fmt.Errorf("Binary expression cannot wrap nil expression")
	}
	typeOfWrapped := reflect.TypeOf(wrapped)
	if typeOfWrapped == aggregateType || typeOfWrapped == avgType || typeOfWrapped == constType {
		return nil
	}
	if typeOfWrapped == binaryType {
		return wrapped.Validate()
	}
	return fmt.Errorf("Binary expression must wrap only aggregate and constant expressions, or other binary expressions that wrap only aggregate or constant expressions, not %v", typeOfWrapped)
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

func (e *binaryExpr) EncodedWidth() int {
	return e.left.EncodedWidth() + e.right.EncodedWidth()
}

func (e *binaryExpr) Update(b []byte, params Params) ([]byte, float64, bool) {
	remain, leftValue, updatedLeft := e.left.Update(b, params)
	remain, rightValue, updatedRight := e.right.Update(remain, params)
	updated := updatedLeft || updatedRight
	return remain, e.calc(leftValue, rightValue), updated
}

func (e *binaryExpr) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	remainB, remainX, remainY := e.left.Merge(b, x, y)
	return e.right.Merge(remainB, remainX, remainY)
}

func (e *binaryExpr) SubMerger(sub Expr) SubMerge {
	if sub.String() == e.String() {
		return e.subMerge
	}
	return combinedSubMerge(e.left.SubMerger(sub), e.left.EncodedWidth(), e.right.SubMerger(sub))
}

func (e *binaryExpr) subMerge(data []byte, other []byte) {
	fmt.Printf("Merging %v\n", e.String())
	e.Merge(data, data, other)
}

func combinedSubMerge(left SubMerge, width int, right SubMerge) SubMerge {
	// Optimization - if left and right are both nil, just return nil
	if left == nil && right == nil {
		return nil
	}
	if right == nil {
		return left
	}
	if left == nil {
		return right
	}
	return func(data []byte, other []byte) {
		left(data, other)
		right(data[width:], other)
	}
}

func (e *binaryExpr) Get(b []byte) (float64, bool, []byte) {
	valueLeft, leftWasSet, remain := e.left.Get(b)
	valueRight, rightWasSet, remain := e.right.Get(remain)
	if !leftWasSet || !rightWasSet {
		return 0, false, remain
	}
	return e.calc(valueLeft, valueRight), true, remain
}

func (e *binaryExpr) String() string {
	return fmt.Sprintf("(%v %v %v)", e.left, e.op, e.right)
}
