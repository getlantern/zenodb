package values

type Value interface {
	Val() float64

	Plus(addend Value) Value
}

type Float float64

func (v Float) Val() float64 {
	return float64(v)
}

func (v Float) Plus(addend Value) Value {
	return Float(float64(v) + addend.Val())
}

type Int int64

func (v Int) Val() float64 {
	return float64(v)
}

func (v Int) Plus(addend Value) Value {
	return Int(int64(v) + int64(addend.(Int)))
}
