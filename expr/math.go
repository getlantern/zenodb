package expr

import (
	"fmt"
	"math"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/msgpack"
)

var unaryMathFNs = map[string]func(float64) float64{
	"LN":    math.Log,
	"LOG2":  math.Log2,
	"LOG10": math.Log10,
}

type unaryMathExpr struct {
	Name    string
	fn      func(float64) float64
	Wrapped Expr
	Width   int
}

func UnaryMath(name string, wrapped interface{}) (Expr, error) {
	fn := unaryMathFNs[name]
	if fn == nil {
		return nil, fmt.Errorf("Unknown math function %v", name)
	}
	_wrapped := exprFor(wrapped)
	return &unaryMathExpr{name, fn, _wrapped, _wrapped.EncodedWidth()}, nil
}

func (e *unaryMathExpr) Validate() error {
	return e.Wrapped.Validate()
}

func (e *unaryMathExpr) EncodedWidth() int {
	return e.Width
}

func (e *unaryMathExpr) Shift() time.Duration {
	return e.Wrapped.Shift()
}

func (e *unaryMathExpr) Update(b []byte, params Params, metadata goexpr.Params) ([]byte, float64, bool) {
	return e.Wrapped.Update(b, params, metadata)
}

func (e *unaryMathExpr) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return e.Wrapped.Merge(b, x, y)
}

func (e *unaryMathExpr) SubMergers(subs []Expr) []SubMerge {
	ssms := e.Wrapped.SubMergers(subs)
	sms := make([]SubMerge, len(subs))
	for i, sub := range subs {
		if e.String() == sub.String() {
			sms[i] = ssms[i]
		}
	}
	return ssms
}

func (e *unaryMathExpr) Get(b []byte) (float64, bool, []byte) {
	val, found, remain := e.Wrapped.Get(b)
	if found {
		val = e.fn(val)
	}
	return val, found, remain
}

func (e *unaryMathExpr) IsConstant() bool {
	return e.Wrapped.IsConstant()
}

func (e *unaryMathExpr) String() string {
	return fmt.Sprintf("%v(%v)", e.Name, e.Wrapped)
}

func (e *unaryMathExpr) DecodeMsgpack(dec *msgpack.Decoder) error {
	m := make(map[string]interface{})
	err := dec.Decode(&m)
	if err != nil {
		return err
	}
	e.Name = m["Name"].(string)
	e.fn = unaryMathFNs[e.Name]
	e.Wrapped = m["Wrapped"].(Expr)
	e.Width = int(m["Width"].(uint64))
	return nil
}
