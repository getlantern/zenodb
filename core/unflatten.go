package core

import (
	"context"
	"github.com/getlantern/zenodb/encoding"
)

func Unflatten() FlatToRow {
	return &unflatten{}
}

type unflatten struct {
	flatRowConnectable
}

func (f *unflatten) Iterate(ctx context.Context, onRow OnRow) error {
	fields := f.GetFields()
	numFields := len(fields)

	return f.iterateParallel(false, ctx, func(row *FlatRow) (bool, error) {
		ts := encoding.TimeFromInt(row.TS)
		outRow := make(Vals, numFields)
		for i, field := range fields {
			outRow[i] = encoding.NewValue(field.Expr, ts, row.Values[i])
		}

		return onRow(row.Key, outRow)
	})
}

func (f *unflatten) String() string {
	return "unflatten"
}
