package expr

import (
	"sync"

	"github.com/robertkrimen/otto"
)

var (
	ottoPool = &sync.Pool{
		New: func() interface{} {
			vm := otto.New()
			vm.Set("Sum", Sum)
			vm.Set("Min", Min)
			vm.Set("Max", Max)
			vm.Set("Count", Count)
			vm.Set("Avg", Avg)
			vm.Set("Add", Add)
			vm.Set("Sub", Sub)
			vm.Set("Mult", Mult)
			vm.Set("Div", Div)
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
