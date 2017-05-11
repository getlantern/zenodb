package core

import (
	"context"
	"fmt"
	"sync/atomic"
)

func Limit(source FlatRowSource, lim int) FlatRowSource {
	return &limit{
		flatRowTransform{source},
		lim,
	}
}

type limit struct {
	flatRowTransform
	limit int
}

func (l *limit) Iterate(ctx context.Context, onFields OnFields, onRow OnFlatRow) error {
	idx := int64(0)
	return l.source.Iterate(ctx, onFields, func(row *FlatRow) (bool, error) {
		newIdx := atomic.AddInt64(&idx, 1)
		oldIdx := int(newIdx - 1)
		if oldIdx < l.limit {
			return onRow(row)
		}
		return stop()
	})
}

func (l *limit) String() string {
	return fmt.Sprintf("limit %d", l.limit)
}
