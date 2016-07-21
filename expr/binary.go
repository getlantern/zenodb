package expr

import (
	"fmt"
	"reflect"
	"sort"
)

type binaryAccumulator struct {
	left  Accumulator
	right Accumulator
}

func (a *binaryAccumulator) Update(params Params) bool {
	updatedLeft := a.left.Update(params)
	updatedRight := a.right.Update(params)
	return updatedLeft || updatedRight
}

func (a *binaryAccumulator) EncodedWidth() int {
	return a.left.EncodedWidth() + a.right.EncodedWidth()
}

func (a *binaryAccumulator) Encode(b []byte) int {
	n := a.left.Encode(b)
	return n + a.right.Encode(b[n:])
}

func (a *binaryAccumulator) InitFrom(b []byte) []byte {
	return a.right.InitFrom(a.left.InitFrom(b))
}

type binaryExpr struct {
	op    string
	left  Expr
	right Expr
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
	if typeOfWrapped == calcType || typeOfWrapped == condType {
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

func (e *binaryExpr) String() string {
	return fmt.Sprintf("(%v %v %v)", e.left, e.op, e.right)
}
