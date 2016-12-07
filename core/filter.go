package core

import (
	"context"
	"github.com/getlantern/bytemap"
)

type Filter struct {
	rowConnectable
	Include func(ctx context.Context, key bytemap.ByteMap, vals Vals) (bool, error)
	Label   string
}

func (f *Filter) Iterate(ctx context.Context, onRow OnRow) error {
	return f.iterateParallel(false, ctx, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		include, err := f.Include(ctx, key, vals)
		if err != nil {
			return false, err
		}
		if include {
			return onRow(key, vals)
		}
		return proceed()
	})
}

func (f *Filter) String() string {
	return f.Label
}
