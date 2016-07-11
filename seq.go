package tdb

import (
	"encoding/binary"
	"math"
	"time"
)

const (
	size64bits = 8
)

var (
	zeroTime = time.Time{}
)

type tsvalue []byte

func newValue(ts time.Time, val float64) tsvalue {
	out := make(sequence, size64bits*2)
	out.setStart(ts)
	out.setValueAtOffset(0, val)
	return tsvalue(out)
}

// sequence represents a time-ordered sequence of values in descending time
// order. The first 8 bytes are the timestamp at which the sequence starts, and
// after that each 8 bytes are a floating point value for the next interval in
// the sequence.
type sequence []byte

func (seq sequence) isValid() bool {
	return seq != nil && len(seq) >= size64bits*2
}

func (seq sequence) start() time.Time {
	if seq == nil {
		return zeroTime
	}
	ts := int64(binary.BigEndian.Uint64(seq))
	s := ts / int64(time.Second)
	ns := ts % int64(time.Second)
	return time.Unix(s, ns)
}

func (seq sequence) setStart(t time.Time) {
	binary.BigEndian.PutUint64(seq, uint64(t.UnixNano()))
}

func (seq sequence) numPeriods() int {
	if seq == nil {
		return 0
	}
	return len(seq)/size64bits - 1
}

func (seq sequence) valueAtTime(t time.Time, resolution time.Duration) float64 {
	if seq == nil {
		return 0
	}
	start := seq.start()
	if t.After(start) {
		return 0
	}
	period := int(start.Sub(t) / resolution)
	return seq.valueAt(period)
}

func (seq sequence) valueAt(period int) float64 {
	if seq == nil {
		return 0
	}
	return seq.valueAtOffset(period * size64bits)
}

func (seq sequence) valueAtOffset(offset int) float64 {
	if seq == nil {
		return 0
	}
	offset = offset + size64bits
	if offset >= len(seq) {
		return 0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(seq[offset:]))
}

func (seq sequence) setValueAtTime(t time.Time, resolution time.Duration, val float64) {
	start := seq.start()
	period := int(start.Sub(t) / resolution)
	seq.setValueAt(period, val)
}

func (seq sequence) setValueAt(period int, val float64) {
	seq.setValueAtOffset(period*size64bits, val)
}

func (seq sequence) setValueAtOffset(offset int, val float64) {
	offset = offset + size64bits
	binary.BigEndian.PutUint64(seq[offset:], math.Float64bits(val))
}

func (seq sequence) set(tsv tsvalue, resolution time.Duration, truncateBefore time.Time) sequence {
	ts := sequence(tsv).start()
	val := sequence(tsv).valueAtOffset(0)

	if !ts.After(truncateBefore) {
		// New value falls outside of truncation range, just truncate existing sequence
		if seq == nil {
			return nil
		}
		return seq.truncate(resolution, truncateBefore)
	}

	if seq == nil {
		// Create a new sequence
		out := make(sequence, 2*size64bits)
		out.setStart(ts)
		out.setValueAt(0, val)
		return out
	}

	start := seq.start()
	if ts.After(start) {
		// Prepend
		delta := ts.Sub(start)
		deltaPeriods := int(delta / resolution)
		out := make(sequence, len(seq)+size64bits*deltaPeriods)
		copy(out[(deltaPeriods+1)*size64bits:], seq[size64bits:])
		out.setStart(ts)
		out.setValueAt(0, val)
		// TODO: optimize this by applying truncation above
		return out.truncate(resolution, truncateBefore)
	}

	// Update existing entry
	out := seq
	period := int(start.Sub(ts) / resolution)
	offset := period * size64bits
	if offset >= len(seq)-size64bits {
		// Grow seq
		out = make(sequence, offset+(2*size64bits))
		copy(out, seq)
	}
	out.setValueAtOffset(offset, val)
	return out.truncate(resolution, truncateBefore)
}

func (seq sequence) truncate(resolution time.Duration, truncateBefore time.Time) sequence {
	if seq == nil {
		return nil
	}
	// New value falls outside of truncation range, truncate existing sequence
	maxPeriods := int(seq.start().Sub(truncateBefore) / resolution)
	if maxPeriods <= 0 {
		// Entire sequence falls outside of truncation range
		return nil
	}
	maxLength := (maxPeriods + 1) * size64bits
	if maxLength >= len(seq) {
		return seq
	}
	return seq[:maxLength]
}
