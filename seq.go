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
	emptySequence = sequence([]byte{})
)

// sequence represents a time-ordered sequence of values in descending time
// order. The first 8 bytes are the timestamp at which the sequence starts, and
// after that each 8 bytes are a floating point value for the next interval in
// the sequence.
type sequence []byte

func (a sequence) isValid() bool {
	return a != nil && len(a) >= size64bits*2
}

func (a sequence) append(b sequence, resolution time.Duration, truncateBefore time.Time) sequence {
	as := a.start()
	bs := b.start()
	if as.Before(bs) {
		// Swap
		a, b = b, a
		as, bs = bs, as
	}
	aPeriods := a.numPeriods()
	maxPeriods := int(as.Sub(truncateBefore)/resolution) + 1
	if maxPeriods <= 0 {
		// Entire sequence falls outside of truncation range
		return emptySequence
	}
	maxLength := (maxPeriods + 1) * size64bits
	if maxPeriods < aPeriods {
		return a[:maxLength]
	}
	if bs.Before(truncateBefore) {
		return a
	}
	gap := int(as.Sub(bs)/resolution) - (len(a) / size64bits) + 1
	gapSize := gap * size64bits
	length := len(a) + len(b) + gapSize - size64bits
	truncateBy := length - maxLength
	if truncateBy > 0 {
		length = maxLength
	} else {
		truncateBy = 0
	}
	result := make(sequence, length)
	copy(result, a)
	copy(result[len(a)+gapSize:], b[size64bits:len(b)-truncateBy])
	return result
}

func (seq sequence) start() time.Time {
	ts := int64(binary.BigEndian.Uint64(seq))
	s := ts / int64(time.Second)
	ns := ts % int64(time.Second)
	return time.Unix(s, ns)
}

func (seq sequence) numPeriods() int {
	return len(seq)/size64bits - 1
}

func (seq sequence) valueAtTime(t time.Time, resolution time.Duration) float64 {
	start := seq.start()
	if t.After(start) {
		return 0
	}
	period := int(start.Sub(t) / resolution)
	return seq.valueAt(period)
}

func (seq sequence) valueAt(period int) float64 {
	offset := (period + 1) * size64bits
	if offset >= len(seq) {
		return 0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(seq[offset:]))
}
