package expr

import (
	"fmt"
	"reflect"
	"time"

	"github.com/getlantern/goexpr"
	"gopkg.in/vmihailenco/msgpack.v2"
)

var binaryExprs = make(map[string]func(left interface{}, right interface{}) *binaryExpr)

func binaryExprFor(op string, left interface{}, right interface{}) *binaryExpr {
	ctor, found := binaryExprs[op]
	if !found {
		return nil
	}
	return ctor(left, right)
}

func registerBinaryExpr(op string, calc calcFN) {
	binaryExprs[op] = func(left interface{}, right interface{}) *binaryExpr {
		return &binaryExpr{
			Op:    op,
			Left:  exprFor(left),
			Right: exprFor(right),
			calc:  calc,
		}
	}
}

type calcFN func(left float64, right float64) float64

type binaryExpr struct {
	Op    string
	Left  Expr
	Right Expr
	calc  calcFN
}

func (e *binaryExpr) Validate() error {
	err := validateWrappedInBinary(e.Left)
	if err == nil {
		err = validateWrappedInBinary(e.Right)
	}
	return err
}

func validateWrappedInBinary(wrapped Expr) error {
	if wrapped == nil {
		return fmt.Errorf("Binary expression cannot wrap nil expression")
	}
	typeOfWrapped := reflect.TypeOf(wrapped)
	if typeOfWrapped == aggregateType || typeOfWrapped == ifType || typeOfWrapped == avgType || typeOfWrapped == constType || typeOfWrapped == shiftType {
		return nil
	}
	if typeOfWrapped == binaryType {
		return wrapped.Validate()
	}
	return fmt.Errorf("Binary expression must wrap only aggregate, if, constant or shift expressions, or other binary expressions that wrap only aggregate or constant expressions, not %v of type %v", wrapped, typeOfWrapped)
}

func (e *binaryExpr) EncodedWidth() int {
	return e.Left.EncodedWidth() + e.Right.EncodedWidth()
}

func (e *binaryExpr) Shift() time.Duration {
	a := e.Left.Shift()
	b := e.Right.Shift()
	if a < b {
		return a
	}
	return b
}

func (e *binaryExpr) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	remain, leftValue, updatedLeft := e.Left.Update(b, params, metadata)
	remain, rightValue, updatedRight := e.Right.Update(remain, params, metadata)
	updated := updatedLeft || updatedRight
	return remain, e.calc(leftValue, rightValue), updated
}

func (e *binaryExpr) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	remainB, remainX, remainY := e.Left.Merge(b, x, y)
	return e.Right.Merge(remainB, remainX, remainY)
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
	left := e.Left.SubMergers(subs)
	right := e.Right.SubMergers(subs)
	for i := range subs {
		result[i] = combinedSubMerge(left[i], e.Left.EncodedWidth(), right[i])
	}

	return result
}

func (e *binaryExpr) subMerge(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
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
		return func(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
			right(data[width:], other, otherRes, metadata)
		}
	}
	return func(data []byte, other []byte, otherRes time.Duration, metadata goexpr.Params) {
		left(data, other, otherRes, metadata)
		right(data[width:], other, otherRes, metadata)
	}
}

func (e *binaryExpr) Get(b []byte) (float64, bool, []byte) {
	valueLeft, leftWasSet, remain := e.Left.Get(b)
	valueRight, rightWasSet, remain := e.Right.Get(remain)
	if !leftWasSet && !rightWasSet {
		return 0, false, remain
	}
	return e.calc(valueLeft, valueRight), true, remain
}

func (e *binaryExpr) IsConstant() bool {
	return e.Left.IsConstant() && e.Right.IsConstant()
}

func (e *binaryExpr) String() string {
	return fmt.Sprintf("(%v %v %v)", e.Left, e.Op, e.Right)
}

func (e *binaryExpr) DecodeMsgpack(dec *msgpack.Decoder) error {
	m := make(map[string]interface{})
	err := dec.Decode(&m)
	if err != nil {
		return err
	}
	e2 := binaryExprFor(m["Op"].(string), m["Left"].(Expr), m["Right"].(Expr))
	if e2 == nil {
		return fmt.Errorf("Unknown binary expression %v", m["Op"])
	}
	e.Op = e2.Op
	e.Left = e2.Left
	e.Right = e2.Right
	e.calc = e2.calc
	return nil
}
