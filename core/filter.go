package core

import (
	"fmt"
	"github.com/getlantern/bytemap"
)

type Filter struct {
	rowConnectable
	Include func(key bytemap.ByteMap, vals Vals) bool
	Label   string
}

func (f *Filter) Iterate(onRow OnRow) error {
	return f.iterateParallel(false, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		if f.Include(key, vals) {
			return onRow(key, vals)
		}
		return proceed()
	})
}

func (f *Filter) String() string {
	return fmt.Sprintf("filter %v", f.Label)
}
