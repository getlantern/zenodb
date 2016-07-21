package tdb

import (
	"github.com/getlantern/tdb/expr"
	"sync"
)

type columnStore interface {
	Update(tsp tsparams)
}

type columnStoreOptions struct {
	Dir        string
	Expr       expr.Expr
	NumPeriods int
	ms         memStore
	mx         sync.RWMutex
}

type memStore map[string]sequence
