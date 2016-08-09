package expr

import (
	"fmt"
	"reflect"

	"github.com/getlantern/goexpr"
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
	if typeOfWrapped == aggregateType || typeOfWrapped == ifType || typeOfWrapped == avgType || typeOfWrapped == constType {
		return nil
	}
	if typeOfWrapped == binaryType {
		return wrapped.Validate()
	}
	return fmt.Errorf("Binary expression must wrap only aggregate, if and constant expressions, or other binary expressions that wrap only aggregate or constant expressions, not %v", typeOfWrapped)
}

func (e *binaryExpr) EncodedWidth() int {
	return e.left.EncodedWidth() + e.right.EncodedWidth()
}

func (e *binaryExpr) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	remain, leftValue, updatedLeft := e.left.Update(b, params, metadata)
	remain, rightValue, updatedRight := e.right.Update(remain, params, metadata)
	updated := updatedLeft || updatedRight
	return remain, e.calc(leftValue, rightValue), updated
}

func (e *binaryExpr) Merge(b []byte, x []byte, y []byte, metadata goexpr.Params) ([]byte, []byte, []byte) {
	remainB, remainX, remainY := e.left.Merge(b, x, y, metadata)
	return e.right.Merge(remainB, remainX, remainY, metadata)
}

func (e *binaryExpr) SubMergers(subs []Expr) []SubMerge {
	result := make([]SubMerge, len(subs))
	// See if any of the subexpressions match top level and if so, ignore others
	for i, sub := range subs {
		if e.String() == sub.String() {
			result[i] = e.subMerge
			return result
		}
	}

	// None of sub expressions match top level, build combined ones
	left := e.left.SubMergers(subs)
	right := e.right.SubMergers(subs)
	for i := range subs {
		result[i] = combinedSubMerge(left[i], e.left.EncodedWidth(), right[i])
	}

	return result
}

func (e *binaryExpr) subMerge(data []byte, other []byte, metadata goexpr.Params) {
	e.Merge(data, data, other, metadata)
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
		return func(data []byte, other []byte, metadata goexpr.Params) {
			right(data[width:], other, metadata)
		}
	}
	return func(data []byte, other []byte, metadata goexpr.Params) {
		left(data, other, metadata)
		right(data[width:], other, metadata)
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
