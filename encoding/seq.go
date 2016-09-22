package encoding

import (
	"fmt"
	"time"

	"github.com/getlantern/goexpr"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/expr"
)

var (
	log = golog.LoggerFor("zenodb.encoding")
)

// Sequence represents a time-ordered sequence of accumulator states in
// descending time order. The first 8 bytes are the timestamp at which the
// Sequence starts, and after that each n bytes are data for the next interval
// in the Sequence, where n is determined by the type of type of Expr.
type Sequence []byte

// NewSequence allocates a new Sequence that holds the given number of periods
// with accumulator states of the given periodWidth.
func NewSequence(periodWidth int, numPeriods int) Sequence {
	return make(Sequence, Width64bits+numPeriods*periodWidth)
}

// Start returns the start time of this Sequence.
func (seq Sequence) Start() time.Time {
	if len(seq) == 0 {
		return zeroTime
	}
	return TimeFromBytes(seq)
}

// SetStart sets the start time of this Sequence.
func (seq Sequence) SetStart(t time.Time) {
	Binary.PutUint64(seq, uint64(t.UnixNano()))
}

// NumPeriods returns the number of periods in this Sequence assuming the given
// periodWidth.
func (seq Sequence) NumPeriods(periodWidth int) int {
	if len(seq) == 0 {
		return 0
	}
	return seq.DataLength() / periodWidth
}

// DataLength returns the number of bytes in this Sequence excluding the start
// time.
func (seq Sequence) DataLength() int {
	return len(seq) - Width64bits
}

// ValueAtTime returns the value at the given time within this sequence,
// extracted using the given Expr and assuming each period represents 1 *
// resolution. If no value is set for the given time, found will be false.
func (seq Sequence) ValueAtTime(t time.Time, e expr.Expr, resolution time.Duration) (val float64, found bool) {
	if len(seq) == 0 {
		return 0, false
	}
	t = RoundTime(t, resolution)
	start := seq.Start()
	if t.After(start) {
		return 0, false
	}
	period := int(start.Sub(t) / resolution)
	return seq.ValueAt(period, e)
}

// ValueAt returns the value at the given period extracted using the given Expr.
// If no value is set for the given period, found will be false.
func (seq Sequence) ValueAt(period int, e expr.Expr) (val float64, found bool) {
	if len(seq) == 0 {
		return 0, false
	}
	if period < 0 {
		return 0, false
	}
	return seq.ValueAtOffset(period*e.EncodedWidth(), e)
}

// DataAt returns the accumulator state at the given period for the given Expr.
// If no data is set for the given time, found will be false.
func (seq Sequence) DataAt(period int, e expr.Expr) (data []byte, found bool) {
	if len(seq) == 0 {
		return nil, false
	}
	if period < 0 {
		return nil, false
	}
	return seq.DataAtOffset(period*e.EncodedWidth(), e)
}

// ValueAtOffset returns the value at the given byte offset in the Sequence (not
// including the start time) extracted using the given Expr.  If no value is set
// for the given offset, found will be false.
func (seq Sequence) ValueAtOffset(offset int, e expr.Expr) (val float64, found bool) {
	if len(seq) == 0 {
		return 0, false
	}
	offset = offset + Width64bits
	if offset >= len(seq) {
		return 0, false
	}
	val, wasSet, _ := e.Get(seq[offset:])
	return val, wasSet
}

// DataAtOffset returns the accumulator state at the given byte offset in the
// Sequence (not including the start time) for the given Expr.  If no data is
// set for the given offset, found will be false.
func (seq Sequence) DataAtOffset(offset int, e expr.Expr) (data []byte, found bool) {
	if len(seq) == 0 {
		return nil, false
	}
	offset = offset + Width64bits
	if offset >= len(seq) {
		return nil, false
	}
	return seq[offset : offset+e.EncodedWidth()], true
}

// UpdateValueAtTime updates the value at the given time by applying the
// supplied Params to the given expression. metadata represents metadata about
// the operation that's used by the Expr as well (e.g. information about the
// dimensions associated to the value). The time is translated to a period by
// assuming that each period represents 1 * resolution of time.
func (seq Sequence) UpdateValueAtTime(t time.Time, resolution time.Duration, e expr.Expr, params expr.Params, metadata goexpr.Params) {
	t = RoundTime(t, resolution)
	start := seq.Start()
	period := int(start.Sub(t) / resolution)
	seq.UpdateValueAt(period, e, params, metadata)
}

// UpdateValueAt updates the value at the given period by applying the supplied
// Params to the given expression. metadata represents metadata about the
// operation that's used by the Expr as well (e.g. information about the
// dimensions associated to the value).
func (seq Sequence) UpdateValueAt(period int, e expr.Expr, params expr.Params, metadata goexpr.Params) {
	seq.UpdateValueAtOffset(period*e.EncodedWidth(), e, params, metadata)
}

// MergeValueAt merges the other accumulator state into the accumulator state at
// the given period, using the given Expr.
func (seq Sequence) MergeValueAt(period int, e expr.Expr, other []byte) {
	seq.MergeValueAtOffset(period*e.EncodedWidth(), e, other)
}

// SubMergeValueAt sub merges the other accumulator state into the accumulator
// state at the given period, using the given Expr and SubMerge. metadata
// represents metadata about the operation that's used by the Expr as well (e.g.
// information about the dimensions associated to the value).
func (seq Sequence) SubMergeValueAt(period int, e expr.Expr, subMerge expr.SubMerge, other []byte, metadata goexpr.Params) {
	seq.SubMergeValueAtOffset(period*e.EncodedWidth(), e, subMerge, other, metadata)
}

// UpdateValueAtOffset updates the value at the given byte offset by applying
// the supplied Params to the given expression. metadata represents metadata
// about the operation that's used by the Expr as well (e.g. information about
// the dimensions associated to the value).
func (seq Sequence) UpdateValueAtOffset(offset int, e expr.Expr, params expr.Params, metadata goexpr.Params) {
	offset = offset + Width64bits
	e.Update(seq[offset:], params, metadata)
}

// MergeValueAtOffset merges the other accumulator state into the accumulator
// state at the given byte offset, using the given Expr.
func (seq Sequence) MergeValueAtOffset(offset int, e expr.Expr, other []byte) {
	offset = offset + Width64bits
	orig := seq[offset:]
	e.Merge(orig, orig, other)
}

// SubMergeValueAtOffset sub merges the other accumulator state into the
// accumulator state at the given byte offset, using the given Expr and
// SubMerge. metadata represents metadata about the operation that's used by the
// Expr as well (e.g. information about the dimensions associated to the value).
func (seq Sequence) SubMergeValueAtOffset(offset int, e expr.Expr, subMerge expr.SubMerge, other []byte, metadata goexpr.Params) {
	offset = offset + Width64bits
	orig := seq[offset:]
	subMerge(orig, other, metadata)
}

// Update unpacks the given TSParams and calls UpdateValue.
func (seq Sequence) Update(tsp TSParams, metadata goexpr.Params, e expr.Expr, resolution time.Duration, truncateBefore time.Time) Sequence {
	ts, params := tsp.TimeAndParams()
	return seq.UpdateValue(ts, params, metadata, e, resolution, truncateBefore)
}

// UpdateValue updates the value at the given time by applying the given params
// using the given Expr. The resolution indicates how wide we assume each period
// of data to be.  Any values in the sequence older than truncateBefore
// including the new value) will be omitted from the sequence. If the sequence
// needs to be grown to accommodate the updated value, it will be. metadata
// represents metadata about the operation that's used by the Expr as well (e.g.
// information about the dimensions associated to the value).
//
// The returned Sequence may reference the same underlying byte array as the
// updated sequence, or it may be a newly allocated byte array (i.e. if the
// sequence grew).
func (seq Sequence) UpdateValue(ts time.Time, params expr.Params, metadata goexpr.Params, e expr.Expr, resolution time.Duration, truncateBefore time.Time) Sequence {
	periodWidth := e.EncodedWidth()
	ts = RoundTime(ts, resolution)
	truncateBefore = RoundTime(truncateBefore, resolution)

	if log.IsTraceEnabled() {
		log.Tracef("Updating sequence starting at %v to %v at %v, truncating before %v", seq.Start().In(time.UTC), params, ts.In(time.UTC), truncateBefore.In(time.UTC))
	}

	if !ts.After(truncateBefore) {
		log.Trace("New value falls outside of truncation range, just truncate existing sequence")
		if len(seq) == 0 {
			return nil
		}
		return seq.Truncate(periodWidth, resolution, truncateBefore)
	}

	sequenceEmpty := len(seq) == 0
	var start time.Time
	var gapPeriods int
	var maxPeriods int
	if !sequenceEmpty {
		start = seq.Start()
		gapPeriods = int(ts.Sub(start) / resolution)
		maxPeriods = int(ts.Sub(truncateBefore) / resolution)
	}
	if sequenceEmpty || start.Before(truncateBefore) || gapPeriods > maxPeriods {
		log.Trace("Creating new sequence")
		out := make(Sequence, Width64bits+periodWidth)
		out.SetStart(ts)
		out.UpdateValueAt(0, e, params, metadata)
		return out
	}

	if ts.After(start) {
		log.Trace("Prepending to sequence")
		numPeriods := seq.NumPeriods(periodWidth) + gapPeriods
		origEnd := len(seq)
		if numPeriods > maxPeriods {
			log.Trace("Truncating existing sequence")
			numPeriods = maxPeriods
			origEnd = Width64bits + periodWidth*(numPeriods-gapPeriods)
		}
		out := NewSequence(periodWidth, numPeriods)
		copy(out[Width64bits+gapPeriods*periodWidth:], seq[Width64bits:origEnd])
		out.SetStart(ts)
		out.UpdateValueAt(0, e, params, metadata)
		return out
	}

	log.Trace("Updating existing entry on sequence")
	out := seq
	period := int(start.Sub(ts) / resolution)
	offset := period * periodWidth
	if offset+periodWidth >= len(seq) {
		// Grow seq
		out = make(Sequence, offset+Width64bits+periodWidth)
		copy(out, seq)
	}
	out.UpdateValueAtOffset(offset, e, params, metadata)
	return out
}

// Merge merges the other Sequence into this Sequence by applying the given
// Expr's merge operator to each period in both Sequences. The resulting
// Sequence will start at the early of the two Sequence's start times, and will
// end at the later of the two Sequence's start times, or at the given
// truncateBefore if that's later.
//
// The returned Sequence may reference the same underlying byte array as one or
// the other Sequence if nothing needed merging, otherwise it will be a newly
// allocated byte array. Merge will NOT update either of the supplied arrays.
func (seq Sequence) Merge(other Sequence, e expr.Expr, resolution time.Duration, truncateBefore time.Time) Sequence {
	if len(seq) == 0 {
		return other
	}
	if len(other) == 0 {
		return seq
	}

	truncateBefore = RoundTime(truncateBefore, resolution)
	sa := seq
	sb := other
	startA := sa.Start()
	startB := sb.Start()
	if startB.After(startA) {
		// Switch
		sa, startA, sb, startB = sb, startB, sa, startA
	}

	if startB.Before(truncateBefore) {
		return sa
	}

	encodedWidth := e.EncodedWidth()
	aPeriods := sa.NumPeriods(encodedWidth)
	bPeriods := sb.NumPeriods(encodedWidth)
	endA := startA.Add(-1 * time.Duration(aPeriods) * resolution)
	endB := startB.Add(-1 * time.Duration(bPeriods) * resolution)
	end := endB
	if endA.Before(endB) {
		end = endA
	}
	totalPeriods := int(startA.Sub(end) / resolution)

	out := make(Sequence, Width64bits+totalPeriods*encodedWidth)
	sout := out

	// Set start
	copy(sout, sa[:Width64bits])
	sout = sout[Width64bits:]
	sa = sa[Width64bits:]
	sb = sb[Width64bits:]

	// Handle starting window with no overlap
	leadEnd := startB
	if startB.Before(endA) {
		leadEnd = endA
	}
	leadNoOverlapPeriods := int(startA.Sub(leadEnd) / resolution)
	if leadNoOverlapPeriods > 0 {
		l := leadNoOverlapPeriods * encodedWidth
		copy(sout, sa[:l])
		sout = sout[l:]
		sa = sa[l:]
	}

	if startB.After(endA) {
		// Handle middle window with overlap
		overlapPeriods := 0
		if endB.After(endA) {
			overlapPeriods = int(startA.Sub(endB) / resolution)
		} else {
			overlapPeriods = int(startA.Sub(endA) / resolution)
		}
		overlapPeriods -= leadNoOverlapPeriods
		for i := 0; i < overlapPeriods; i++ {
			sout, sa, sb = e.Merge(sout, sa, sb)
		}
	} else if startB.Before(endA) {
		// Handle gap
		gapPeriods := int(endA.Sub(startB) / resolution)
		gap := gapPeriods * encodedWidth
		sout = sout[gap:]
	}

	// Handle end window with no overlap
	if endA.Before(endB) {
		copy(sout, sa)
	} else if endB.Before(endA) {
		copy(sout, sb)
	}

	return out
}

// Truncate truncates all periods in the Sequence that come before the given
// truncateBefore.
func (seq Sequence) Truncate(periodWidth int, resolution time.Duration, truncateBefore time.Time) Sequence {
	if len(seq) == 0 {
		return nil
	}
	truncateBefore = RoundTime(truncateBefore, resolution)
	maxPeriods := int(seq.Start().Sub(truncateBefore) / resolution)
	if maxPeriods <= 0 {
		// Entire sequence falls outside of truncation range
		return nil
	}
	maxLength := Width64bits + maxPeriods*periodWidth
	if maxLength >= len(seq) {
		return seq
	}
	return seq[:maxLength]
}

// String provides a string representation of this Sequence assuming that it
// holds data for the given Expr.
func (seq Sequence) String(e expr.Expr) string {
	if len(seq) == 0 {
		return ""
	}

	values := ""

	numPeriods := seq.NumPeriods(e.EncodedWidth())
	for i := 0; i < numPeriods; i++ {
		if i > 0 {
			values += " "
		}
		val, _ := seq.ValueAt(i, e)
		values += fmt.Sprint(val)
	}
	return fmt.Sprintf("%v at %v: %v", e, seq.Start(), values)
}
