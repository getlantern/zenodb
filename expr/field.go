package expr

// FIELD creates an Expr that obtains its value from a named field.
func FIELD(name string) Expr {
	return &field{name}
}

type fieldAccumulator struct {
	name  string
	value float64
}

func (a *fieldAccumulator) Update(params Params) {
	a.value = params.Get(a.name)
}

func (a *fieldAccumulator) Get() float64 {
	return a.value
}

func (a *fieldAccumulator) EncodedWidth() int {
	return 0
}

func (a *fieldAccumulator) Encode(b []byte) int {
	return 0
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

func (e *field) Validate() error {
	return nil
}

func (e *field) DependsOn() []string {
	return []string{e.name}
}

func (e *field) String() string {
	return e.name
}
