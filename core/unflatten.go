package core

import (
	"context"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
)

func Unflatten(fields ...Field) FlatToRow {
	return &unflatten{fields: fields}
}

type unflatten struct {
	flatRowConnectable
	fields []Field
}

func (f *unflatten) Iterate(ctx context.Context, onRow OnRow) error {
	inFields := f.GetFields()
	numIn := len(inFields)
	numFields := len(f.fields)

	return f.iterateParallel(false, ctx, func(row *FlatRow) (bool, error) {
		ts := encoding.TimeFromInt(row.TS)
		outRow := make(Vals, numFields)
		params := expr.Map(make(map[string]float64, numIn))
		for i, field := range inFields {
			params[field.Name] = row.Values[i]
		}
		for i, field := range f.fields {
			outRow[i] = encoding.NewValue(field.Expr, ts, params, row.Key)
		}
		return onRow(row.Key, outRow)
	})
}

func (f *unflatten) String() string {
	return "unflatten"
}
