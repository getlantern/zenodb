package expr

type updateFN func(current float64, next float64) float64

// aggregate creates an Expr that obtains its value by doing aggregation
func aggregate(expr interface{}, defaultValue float64, update updateFN) Expr {
	return &agg{exprFor(expr), defaultValue, update}
}

type aggregateAccumulator struct {
	wrapped      Accumulator
	update       updateFN
	defaultValue float64
	value        float64
}

func (a *aggregateAccumulator) Update(params Params) {
	a.wrapped.Update(params)
	a.value = a.update(a.value, a.wrapped.Get())
}

func (a *aggregateAccumulator) Get() float64 {
	if a.value == a.defaultValue {
		return 0
	}
	return a.value
}

type agg struct {
	wrapped      Expr
	defaultValue float64
	update       updateFN
}

func (e *agg) Accumulator() Accumulator {
	return &aggregateAccumulator{
		wrapped:      e.wrapped.Accumulator(),
		update:       e.update,
		defaultValue: e.defaultValue,
		value:        e.defaultValue,
	}
}

func (e *agg) DependsOn() []string {
	return e.wrapped.DependsOn()
}