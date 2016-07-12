package expr

// FIELD creates an Expr that obtains its value from a named field.
func FIELD(name string) Expr {
	return &field{name}
}

type fieldAccumulator struct {
	name  string
	value Value
}

func (a *fieldAccumulator) Update(params Params) {
	a.value = params.Get(a.name)
}

func (a *fieldAccumulator) Get() float64 {
	return a.value.Get()
}

func (a *fieldAccumulator) Bytes() []byte {
	return nil
}

func (a *fieldAccumulator) InitFrom(b []byte) []byte {
	return b
}

type field struct {
	name string
}

func (e *field) Accumulator() Accumulator {
	return &fieldAccumulator{name: e.name}
}

func (e *field) DependsOn() []string {
	return []string{e.name}
}

func (e *field) String() string {
	return e.name
}
