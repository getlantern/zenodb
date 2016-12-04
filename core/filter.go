package core

import (
	"github.com/getlantern/bytemap"
)

type Filter struct {
	Join
	Include func(dims bytemap.ByteMap, vals Vals) bool
}

func (f *Filter) Iterate(onRow OnRow) error {
	return f.iterateParallel(false, func(key bytemap.ByteMap, vals Vals) {
		if f.Include(key, vals) {
			onRow(key, vals)
		}
	})
}
