package core

import (
	"sync/atomic"
)

func Limit(offset int, lim int) FlatToFlat {
	return &limit{offset: offset, limit: lim}
}

type limit struct {
	flatRowConnectable
	offset int
	limit  int
}

func (l *limit) Iterate(onRow OnFlatRow) error {
	idx := int64(0)

	return l.iterateParallel(true, func(row *FlatRow) {
		newIdx := atomic.AddInt64(&idx, 1)
		oldIdx := int(newIdx - 1)
		// TODO: allow stopping iteration here
		if oldIdx >= l.offset && (l.limit == 0 || oldIdx < l.limit+l.offset) {
			onRow(row)
		}
	})
}
