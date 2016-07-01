package expr

import (
	"sync"

	"github.com/robertkrimen/otto"
)

var (
	ottoPool = &sync.Pool{
		New: func() interface{} {
			vm := otto.New()
			vm.Set("SUM", SUM)
			vm.Set("MIN", MIN)
			vm.Set("MAX", MAX)
			vm.Set("COUNT", COUNT)
			vm.Set("AVG", AVG)
			vm.Set("ADD", ADD)
			vm.Set("SUB", SUB)
			vm.Set("MULT", MULT)
			vm.Set("DIV", DIV)
			return vm
		},
	}
)

// JS creates an expression based on a JavaScript statement
func JS(js string) (Expr, error) {
	vm := ottoPool.Get().(*otto.Otto)
	out, err := vm.Run(js)
	if err != nil {
		return nil, err
	}
	e, err := out.Export()
	if err != nil {
		return nil, err
	}
	return e.(Expr), nil
}
