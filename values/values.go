package values

type Value interface {
	Val() float64

	Add(addend float64) Value
}

type Float float64

func (v Float) Val() float64 {
	return float64(v)
}

func (v Float) Add(addend float64) Value {
	return Float(v.Val() + addend)
}
