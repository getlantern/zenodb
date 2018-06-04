package core

import (
	"context"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/expr"
)

func Flatten(source RowSource) FlatRowSource {
	return &flatten{rowTransform{source}}
}

type flatten struct {
	rowTransform
}

func (f *flatten) Iterate(ctx context.Context, onMetadata OnMetadata, onRow OnFlatRow) error {
	guard := Guard(ctx)

	resolution := f.GetResolution()

	var fields Fields
	var numFields int

	return f.source.Iterate(ctx, func(md *Metadata) error {
		fields = md.Fields
		numFields = len(fields)
		// Transform to flattened version of fields
		outFields := make(Fields, 0, len(fields))
		for _, field := range fields {
			outFields = append(outFields, NewField(field.Name, expr.FIELD(field.Name)))
		}
		return onMetadata(md.WithFields(outFields))
	}, func(key bytemap.ByteMap, vals Vals) (bool, error) {
		var until time.Time
		var asOf time.Time
		// Figure out total time range
		for i, field := range fields {
			val := vals[i]
			e := field.Expr
			width := e.EncodedWidth()
			if val.NumPeriods(width) == 0 {
				continue
			}
			newUntil := val.Until()
			newAsOf := val.AsOf(width, resolution)
			if newUntil.After(until) {
				until = newUntil
			}
			if asOf.IsZero() || newAsOf.Before(asOf) {
				asOf = newAsOf
			}
		}

		// Iterate
		ts := asOf
		for ; !ts.After(until); ts = ts.Add(resolution) {
			tsNanos := ts.UnixNano()
			row := &FlatRow{
				TS:     tsNanos,
				Key:    key,
				Values: make([]float64, numFields),
				fields: fields,
			}
			anyNonConstantValueFound := false
			for i, field := range fields {
				val, found := vals[i].ValueAtTime(ts, field.Expr, resolution)
				if found && !field.Expr.IsConstant() {
					anyNonConstantValueFound = true
				}
				row.Values[i] = val
			}
			if anyNonConstantValueFound {
				more, err := onRow(row)
				if !more || err != nil {
					return more, err
				}
			}
		}

		return guard.Proceed()
	})
}

func (f *flatten) String() string {
	return "flatten"
}
