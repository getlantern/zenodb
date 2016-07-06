package tdb

import (
	"encoding/binary"
	"math"
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
	b.vals = make([]expr.Accumulator, 0, len(insert.t.fields))
	for _, field := range insert.t.fields {
		b.vals = append(b.vals, field.Accumulator())
	}
	b.update(insert)
}

func (b *bucket) update(insert *insert) {
	insert.bucket = b
	for i := range insert.t.fields {
		b.vals[i].Update(insert)
	}
}

func (b *bucket) toSequences(resolution time.Duration) []sequence {
	bufs := make([][]byte, 0, len(b.vals))
	for i := 0; i < len(b.vals); i++ {
		// Pre-allocate a largish amount of space to avoid having to grow the buffer
		// too often.
		buf := make([]byte, 1024)
		// Write the starting time of the sequence
		binary.BigEndian.PutUint64(buf, uint64(b.start.UnixNano()))
		bufs = append(bufs, buf)
	}

	// Write all values
	i := 1
	for {
		offset := i * size64bits
		i++
		for j, buf := range bufs {
			if offset >= len(buf) {
				newBuf := make([]byte, offset+1024)
				copy(newBuf, buf)
				buf = newBuf
			}
			binary.BigEndian.PutUint64(buf[offset:], math.Float64bits(b.vals[j].Get()))
		}

		if b.prev == nil {
			break
		}

		// Fill gaps
		delta := int(b.start.Sub(b.prev.start)/resolution) - 1
		i += delta

		// Continue with previous bucket
		b = b.prev
	}

	result := make([]sequence, 0, len(bufs))
	for _, buf := range bufs {
		result = append(result, sequence(buf[:i*size64bits]))
	}
	return result
}
