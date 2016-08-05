package expr

// IsField checks whether the given expression is a field expression and if so,
// returns the name of the field.
func IsField(e Expr) (string, bool) {
	f, ok := e.(*field)
	if !ok {
		return "", false
	}
	return f.name, true
}

// FIELD creates an Expr that obtains its value from a named field.
func FIELD(name string) Expr {
	return &field{name}
}

type fieldAccumulator struct {
	name  string
	value float64
}

type field struct {
	name string
}

func (e *field) Validate() error {
	return nil
}

func (e *field) DependsOn() []string {
	return []string{e.name}
}

func (e *field) EncodedWidth() int {
	return 0
}

func (e *field) Update(b []byte, params Params) ([]byte, float64, bool) {
	val, ok := params.Get(e.name)
	return b, val, ok
}

func (e *field) Merge(b []byte, x []byte, y []byte) ([]byte, []byte, []byte) {
	return b, x, y
}

func (e *field) SubMerger(sub Expr) SubMerge {
	return nil
}

func (e *field) Get(b []byte) (float64, bool, []byte) {
	return 0, false, b
}

func (e *field) String() string {
	return e.name
}
