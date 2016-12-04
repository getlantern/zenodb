package core

import (
	"github.com/getlantern/bytemap"
)

type IncludeTest func(key bytemap.ByteMap, vals Vals) bool

func Filter(include IncludeTest) ConnectableRowSource {
	return &filter{Include: include}
}

type filter struct {
	Join
	Include IncludeTest
}

func (f *filter) Iterate(onRow OnRow) error {
	return f.iterateParallel(false, func(key bytemap.ByteMap, vals Vals) {
		if f.Include(key, vals) {
			onRow(key, vals)
		}
	})
}
