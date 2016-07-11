package tdb

import (
	"time"

	"github.com/getlantern/tdb/expr"
)

type bucket struct {
	start time.Time
	prev  *bucket
	vals  []expr.Accumulator
}

func (b *bucket) init(insert *insert) {
	insert.bucket = b
	b.vals = make([]expr.Accumulator, 0, len(insert.t.Fields))
	for _, field := range insert.t.Fields {
		b.vals = append(b.vals, field.Accumulator())
	}
	b.update(insert)
}

func (b *bucket) update(insert *insert) {
	insert.bucket = b
	for i := range insert.t.Fields {
		b.vals[i].Update(insert)
	}
}

func (b *bucket) toValues(resolution time.Duration) [][]tsvalue {
	vals := make([][]tsvalue, len(b.vals))

	// Write all values
	for {
		for i, val := range b.vals {
			vals[i] = append(vals[i], newTSValue(b.start, val.Get()))
		}
		if b.prev == nil {
			break
		}
		// Continue with previous bucket
		b = b.prev
	}

	return vals
}
