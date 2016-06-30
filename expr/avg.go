package expr

// Avg creates an Expr that obtains its value by averaging the values of the
// given expression or field.
func Avg(expr interface{}) Expr {
	return &avg{exprFor(expr)}
}

type avgAccumulator struct {
	wrapped Accumulator
	count   float64
	total   float64
}

func (a *avgAccumulator) Update(params Params) {
	a.wrapped.Update(params)
	a.count++
	a.total += a.wrapped.Get()
}

func (a *avgAccumulator) Get() float64 {
	if a.count == 0 {
		return 0
	}
	return a.total / a.count
}

type avg struct {
	wrapped Expr
}

func (e *avg) Accumulator() Accumulator {
	return &avgAccumulator{wrapped: e.wrapped.Accumulator()}
}

func (e *avg) DependsOn() []string {
	return e.wrapped.DependsOn()
}
