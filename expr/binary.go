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

func (e *binaryExpr) Merge(x []byte, y []byte) ([]byte, []byte) {
	remainX, remainY := e.left.Merge(x, y)
	return e.right.Merge(remainX, remainY)
}

func (e *binaryExpr) Get(b []byte) (float64, []byte) {
	valueLeft, remain := e.left.Get(b)
	valueRight, remain := e.right.Get(remain)
	return e.calc(valueLeft, valueRight), remain
}

func (e *binaryExpr) String() string {
	return fmt.Sprintf("(%v %v %v)", e.left, e.op, e.right)
}
