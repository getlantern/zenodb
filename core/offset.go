package core

import (
	"context"
	"fmt"
	"sync/atomic"
)

func Offset(source FlatRowSource, off int) FlatRowSource {
	return &offset{
		flatRowTransform{source},
		off,
	}
}

type offset struct {
	flatRowTransform
	offset int
}

func (o *offset) Iterate(ctx context.Context, onRow OnFlatRow) error {
	idx := int64(0)

	return o.source.Iterate(ctx, func(row *FlatRow) (bool, error) {
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
