package core

import (
	"context"
	"fmt"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
)

func Unflatten(source FlatRowSource, fields FieldSource) RowSource {
	return &unflatten{
		flatRowTransform{source},
		fields,
	}
}

func UnflattenOptimized(source FlatRowSource) RowSource {
	fl, ok := source.(Transform)
	if ok {
		rs, ok := fl.GetSource().(RowSource)
		if ok {
			// We're attempting to unflatten a flatten, just go back to the original source and skip the flatten/unflatten cycle
			return rs
		}
	}
	return Unflatten(source, PassthroughFieldSource)
}

type unflatten struct {
	flatRowTransform
	fields FieldSource
}

func (f *unflatten) Iterate(ctx context.Context, onFields OnFields, onRow OnRow) error {
	var inFields, outFields Fields
	var numIn, numOut int

	return f.source.Iterate(ctx, func(fields Fields) error {
		inFields = fields
		var err error
		outFields, err = f.fields.Get(inFields)
		if err != nil {
			return err
		}
		if len(outFields) == 0 {
			// default to inFields
			outFields = inFields
		}
		numIn = len(inFields)
		numOut = len(outFields)
		return onFields(outFields)
	}, func(row *FlatRow) (bool, error) {
		ts := encoding.TimeFromInt(row.TS)
		outRow := make(Vals, numOut)
		params := expr.Map(make(map[string]float64, numIn))
		for i, field := range inFields {
			name := field.Name
			params[name] = row.Values[i]
		}
		for i, field := range outFields {
			outRow[i] = encoding.NewValue(field.Expr, ts, params, row.Key)
		}
		return onRow(row.Key, outRow)
	})
}

func (f *unflatten) String() string {
	if f.fields == PassthroughFieldSource {
		return "unflatten all"
	}
	return fmt.Sprintf("unflatten to %v", f.fields)
}
