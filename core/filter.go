package core

import (
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
)

func RowFilter(source RowSource, label string, include func(ctx context.Context, key bytemap.ByteMap, fields Fields, vals Vals) (bytemap.ByteMap, Vals, error)) RowSource {
	return &rowFilter{
		rowTransform{source},
		include,
		label,
	}
}

type rowFilter struct {
	rowTransform
	Include func(ctx context.Context, key bytemap.ByteMap, fields Fields, vals Vals) (bytemap.ByteMap, Vals, error)
	Label   string
}

func (f *rowFilter) Iterate(ctx context.Context, onMetadata OnMetadata, onRow OnRow) error {
	guard := Guard(ctx)

	var fields Fields
	return f.source.Iterate(ctx, func(md *Metadata) error {
		fields = md.Fields
		return onMetadata(md)
	}, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		var err error
		key, vals, err = f.Include(ctx, key, fields, vals)
		if err != nil {
			return false, err
		}
		if key != nil {
			return onRow(key, vals)
		}
		return guard.Proceed()
	})
}

func (f *rowFilter) String() string {
	return fmt.Sprintf("rowFilter %v", f.Label)
}

func FlatRowFilter(source FlatRowSource, label string, include func(ctx context.Context, row *FlatRow, fields Fields) (*FlatRow, error)) FlatRowSource {
	return &flatRowFilter{
		flatRowTransform{source},
		include,
		label,
	}
}

type flatRowFilter struct {
	flatRowTransform
	Include func(ctx context.Context, row *FlatRow, fields Fields) (*FlatRow, error)
	Label   string
}

func (f *flatRowFilter) Iterate(ctx context.Context, onMetadata OnMetadata, onRow OnFlatRow) error {
	guard := Guard(ctx)

	var fields Fields
	return f.source.Iterate(ctx, func(md *Metadata) error {
		fields = md.Fields
		return onMetadata(md)
	}, func(row *FlatRow) (bool, error) {
		var err error
		row, err = f.Include(ctx, row, fields)
		if err != nil {
			return false, err
		}
		if row != nil {
			return onRow(row)
		}
		return guard.Proceed()
	})
}

func (f *flatRowFilter) String() string {
	return fmt.Sprintf("flatrowFilter %v", f.Label)
}
