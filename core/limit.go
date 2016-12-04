package core

import (
	"sync/atomic"
)

type Limit struct {
	Join
	Limit  int
	Offset int
}

func (l *Limit) Iterate(onRow func(row *FlatRow)) error {
	idx := int64(0)

	return l.iterateParallelFlat(true, func(row *FlatRow) {
		newIdx := atomic.AddInt64(&idx, 1)
		oldIdx := int(newIdx - 1)
		// TODO: allow stopping iteration here
		if oldIdx >= l.Offset && oldIdx < l.Limit+l.Offset {
			onRow(row)
		}
	})
}
