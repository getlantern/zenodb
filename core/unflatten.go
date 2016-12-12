package core

import (
	"context"
	"fmt"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
)

func Unflatten(source FlatRowSource, fields ...Field) RowSource {
	return &unflatten{
		flatRowTransform{source},
		fields,
	}
}

type unflatten struct {
	flatRowTransform
	fields Fields
}

func (f *unflatten) Iterate(ctx context.Context, onRow OnRow) error {
	inFields := f.source.GetFields()
	numIn := len(inFields)
	numFields := len(f.fields)

	return f.source.Iterate(ctx, func(row *FlatRow) (bool, error) {
		ts := encoding.TimeFromInt(row.TS)
		outRow := make(Vals, numFields)
		params := expr.Map(make(map[string]float64, numIn))
		for i, field := range inFields {
			name := field.Name
			if name == "_points" {
				// Hack for _points magic field
				name = "_point"
			}
			params[name] = row.Values[i]
		}
		for i, field := range f.fields {
			outRow[i] = encoding.NewValue(field.Expr, ts, params, row.Key)
		}
		return onRow(row.Key, outRow)
	})
}

func (f *unflatten) GetFields() Fields {
	return f.fields
}

func (f *unflatten) String() string {
	return fmt.Sprintf("unflatten to %v", f.fields)
}
