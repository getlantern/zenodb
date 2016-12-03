package pipeline

import (
	"github.com/getlantern/bytemap"
	"time"
)

type FlatRow struct {
	TS  int64
	Key bytemap.ByteMap
	// Values for each field
	Values []float64
	// For crosstab queries, this contains the total value for each field
	Totals []float64
	fields []Field
}

// Get implements the interface method from goexpr.Params
func (row *FlatRow) Get(param string) interface{} {
	// First look at values
	for i, field := range row.fields {
		if field.Name == param {
			return row.Values[i]
		}
	}

	// Then look at key
	return row.Key.Get(param)
}

type Flatten struct {
	Join
}

func (f *Flatten) Iterate(onRow func(row *FlatRow)) error {
	// TODO: this assumes that all sources have same metadata, should check here
	// to make sure.
	fields := f.sources[0].Fields()
	numFields := len(fields)
	resolution := f.sources[0].Resolution()
	return f.iterateParallel(func(key bytemap.ByteMap, vals Vals) {
		var until time.Time{}
		var asOf time.Time{}
		for i, field := range fields {
			newUntil := vals[0].Until()
			newAsOf := vals[0].AsOf(field.Expr.EncodedWidth(), resolution)
			if newUntil.After(until) {
				until = newUntil
			}
			if asOf.IsZero() || newAsOf.Before(asOf) {
				asOf = newAsOf
			}
		}
		ts := asOf
		for ;!ts.After(until); ts = ts.Add(resolution) {
			row := &FlatRow{
				TS: ts,
				Key: key,
				Values: make([]float64, numFields),
				fields: fields,
			}
			for i, field := range fields {
				val, _ := vals[i].ValueAtTime(ts, e, resolution)
				row.Values
		}
		if f.Include(key, vals) {
			onRow(key, vals)
		}
	})
}
