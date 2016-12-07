package core

import (
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
)

type RowFilter struct {
	rowConnectable
	Include func(ctx context.Context, key bytemap.ByteMap, vals Vals) (bytemap.ByteMap, Vals, error)
	Label   string
}

func (f *RowFilter) Iterate(ctx context.Context, onRow OnRow) error {
	return f.iterateParallel(false, ctx, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		var err error
		key, vals, err = f.Include(ctx, key, vals)
		if err != nil {
			return false, err
		}
		if key != nil {
			return onRow(key, vals)
		}
		return proceed()
	})
}

func (f *RowFilter) String() string {
	return fmt.Sprintf("rowfilter %v", f.Label)
}

type FlatRowFilter struct {
	flatRowConnectable
	Include func(ctx context.Context, row *FlatRow) (*FlatRow, error)
	Label   string
}

func (f *FlatRowFilter) Iterate(ctx context.Context, onRow OnFlatRow) error {
	return f.iterateParallel(false, ctx, func(row *FlatRow) (bool, error) {
		var err error
		row, err = f.Include(ctx, row)
		if err != nil {
			return false, err
		}
		if row != nil {
			return onRow(row)
		}
		return proceed()
	})
}

func (f *FlatRowFilter) String() string {
	return fmt.Sprintf("flatrowfilter %v", f.Label)
}
