package core

import (
	"github.com/getlantern/bytemap"
	"time"
)

func Flatten() RowToFlat {
	return &flatten{}
}

type flatten struct {
	rowConnectable
}

func (f *flatten) Iterate(onRow OnFlatRow) error {
	fields := f.GetFields()
	numFields := len(fields)
	resolution := f.GetResolution()

	return f.iterateParallel(false, func(key bytemap.ByteMap, vals Vals) (bool, error) {
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
			anyValueFound := false
			for i, field := range fields {
				val, found := vals[i].ValueAtTime(ts, field.Expr, resolution)
				if found {
					anyValueFound = true
				}
				row.Values[i] = val
			}
			if anyValueFound {
				return onRow(row)
			}
		}

		return proceed()
	})
}

func (f *flatten) String() string {
	return "flatten"
}
