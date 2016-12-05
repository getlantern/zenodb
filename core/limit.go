package core

import (
	"context"
	"fmt"
	"sync/atomic"
)

func Limit(lim int) FlatToFlat {
	return &limit{limit: lim}
}

type limit struct {
	flatRowConnectable
	limit int
}

func (l *limit) Iterate(ctx context.Context, onRow OnFlatRow) error {
	idx := int64(0)

	return l.iterateParallel(true, ctx, func(row *FlatRow) (bool, error) {
		newIdx := atomic.AddInt64(&idx, 1)
		oldIdx := int(newIdx - 1)
		// TODO: allow stopping iteration here
		if oldIdx < l.limit {
			return onRow(row)
		}
		return stop()
	})
}

func (l *limit) String() string {
	return fmt.Sprintf("limit %d", l.limit)
}
