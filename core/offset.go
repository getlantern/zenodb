package core

import (
	"context"
	"fmt"
	"sync/atomic"
)

func Offset(off int) FlatToFlat {
	return &offset{offset: off}
}

type offset struct {
	flatRowConnectable
	offset int
}

func (o *offset) Iterate(ctx context.Context, onRow OnFlatRow) error {
	idx := int64(0)

	return o.iterateParallel(true, ctx, func(row *FlatRow) (bool, error) {
		newIdx := atomic.AddInt64(&idx, 1)
		oldIdx := int(newIdx - 1)
		// TODO: allow stopping iteration here
		if oldIdx >= o.offset {
			return onRow(row)
		}
		return proceed()
	})
}

func (o *offset) String() string {
	return fmt.Sprintf("offset %d", o.offset)
}
