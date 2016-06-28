package tdb

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/oxtoacart/tdb/values"
)

const (
	size64bits = 8
)

var (
	emptySequence = []byte{}
)

// sequence represents a time-ordered sequence of values in descending time
// order. The first 8 bytes are the timestamp at which the sequence starts, and
// after that each 8 bytes are a floating point value for the next interval in
// the sequence.
type sequence []byte

func (b *bucket) toSequences(resolution time.Duration) map[string]sequence {
	bufs := make(map[string][]byte, len(b.vals))
	for field := range b.vals {
		bufs[field] = b.newBuffer()
	}

	// Write all values
	i := 1
	for {
		offset := i * size64bits
		i++
		for field, buf := range bufs {
			val := b.vals[field]
			if val == nil {
				val = values.Float(0)
				b.vals[field] = val
			}
			bufs[field] = b.collect(buf, offset, val)
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

	out := make(map[string]sequence, len(bufs))
	for field, buf := range bufs {
		out[field] = sequence(buf[:i*size64bits])
	}
	return out
}

func (b *bucket) newBuffer() []byte {
	// Pre-allocate a largish amount of space to avoid having to grow the buffer
	// too often.
	buf := make([]byte, 1024)
	// Write the starting time of the sequence
	binary.BigEndian.PutUint64(buf, uint64(b.start.UnixNano()))
	return buf
}

func (b *bucket) collect(buf []byte, offset int, val values.Value) []byte {
	if offset >= len(buf) {
		newBuf := make([]byte, offset+1024)
		copy(newBuf, buf)
		buf = newBuf
	}
	binary.BigEndian.PutUint64(buf[offset:], math.Float64bits(val.Val()))
	return buf
}

func (a sequence) isValid() bool {
	return a != nil && len(a) >= size64bits*2
}

func (a sequence) append(b sequence, resolution time.Duration) sequence {
	as := a.start()
	bs := b.start()
	if as.Before(bs) {
		// Swap
		a, b = b, a
		as, bs = bs, as
	}
	gap := int(as.Sub(bs)/resolution) - (len(a) / size64bits) + 1
	gapSize := gap * size64bits
	result := make(sequence, len(a)+len(b)+gapSize-size64bits)
	copy(result, a)
	copy(result[len(a)+gapSize:], b[size64bits:])
	return result
}

func (seq sequence) start() time.Time {
	ts := int64(binary.BigEndian.Uint64(seq))
	s := ts / int64(time.Second)
	ns := ts % int64(time.Second)
	return time.Unix(s, ns)
}

func (seq sequence) numBuckets() int {
	return len(seq)/size64bits - 1
}

func (seq sequence) valueAtTime(t time.Time, resolution time.Duration) float64 {
	start := seq.start()
	if t.After(start) {
		return 0
	}
	bucket := int(start.Sub(t) / resolution)
	return seq.valueAt(bucket)
}

func (seq sequence) valueAt(bucket int) float64 {
	offset := (bucket + 1) * size64bits
	if offset >= len(seq) {
		return 0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(seq[offset:]))
}
