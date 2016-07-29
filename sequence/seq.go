package sequence

import (
	"fmt"
	"time"

	"github.com/getlantern/golog"
	"github.com/getlantern/tdb/enc"
	"github.com/getlantern/tdb/expr"
)

var (
	log = golog.LoggerFor("sequence")
)

// Seq represents a time-ordered sequence of expression states in descending
// time order. The first 8 bytes are the timestamp at which the
// Seq starts, and after that each n bytes are the encoded data for one interval
// in the sequence, where n is determined by the expression.
//
// For sparsely populated time series, this leaves a lot of blank space in the
// array, however when stored on disk the arrays are compressed with snappy and
// the blank space compresses quite well.
//
// The main benefit to this structure is that lookups and modifications for
// specific time periods can be performed without having to iterate through the
// sequence.
type Seq []byte

// New allocates a new Seq sized to hold the given numPeriods of the specified
// period widths.
func New(periodWidth int, numPeriods int) Seq {
	return make(Seq, enc.Width64Bits+numPeriods*periodWidth)
}

// Start returns the start time of the sequence.
func (seq Seq) Start() time.Time {
	if seq == nil {
		return enc.ZeroTime
	}
	return enc.TimeFromBytes(seq)
}

// SetStart sets the start time for the sequence.
func (seq Seq) SetStart(t time.Time) {
	enc.Binary.PutUint64(seq, uint64(t.UnixNano()))
}

// NumPeriods returns the number of periods in the sequence, given a specific
// periodWidth.
func (seq Seq) NumPeriods(periodWidth int) int {
	if seq == nil {
		return 0
	}
	return seq.DataLength() / periodWidth
}

// DataLength returns the sequence's length without start time
func (seq Seq) DataLength() int {
	return len(seq) - enc.Width64Bits
}

// ValueAtTime returns the value at the given time using the given expression
// and assuming the given time resolution (i.e. each period corresponds to
// 1 * resolution).
func (seq Seq) ValueAtTime(t time.Time, e expr.Expr, resolution time.Duration) (float64, bool) {
	if seq == nil {
		return 0, false
	}
	start := seq.Start()
	if t.After(start) {
		return 0, false
	}
	period := int(start.Sub(t) / resolution)
	return seq.ValueAt(period, e)
}

// ValueAt returns the value at the given period offset from the sequence's
// start using the given expression.
func (seq Seq) ValueAt(period int, e expr.Expr) (float64, bool) {
	if seq == nil {
		return 0, false
	}
	if period < 0 {
		return 0, false
	}
	return seq.ValueAtOffset(period*e.EncodedWidth(), e)
}

// ValueAtOffset returns the value at the given byte offset from the sequence's
// start using the given expression.
func (seq Seq) ValueAtOffset(offset int, e expr.Expr) (float64, bool) {
	if seq == nil {
		return 0, false
	}
	offset = offset + enc.Width64Bits
	if offset >= len(seq) {
		return 0, false
	}
	val, wasSet, _ := e.Get(seq[offset:])
	return val, wasSet
}

// UpdateValueAtTime updates the value at the given time by applying the given
// params using the given expression. Resolution indicates how much time each
// period represents.
func (seq Seq) UpdateValueAtTime(t time.Time, resolution time.Duration, e expr.Expr, params expr.Params) {
	start := seq.Start()
	period := int(start.Sub(t) / resolution)
	seq.UpdateValueAt(period, e, params)
}

// UpdateValueAt updates the value at the given period offset by applying the
// given params using the given expression.
func (seq Seq) UpdateValueAt(period int, e expr.Expr, params expr.Params) {
	seq.UpdateValueAtOffset(period*e.EncodedWidth(), e, params)
}

// UpdateValueAtOffset updates the value at the given byte offset by applying
// the given params using the given expression.
func (seq Seq) UpdateValueAtOffset(offset int, e expr.Expr, params expr.Params) {
	offset = offset + enc.Width64Bits
	e.Update(seq[offset:], params)
}

// Update updates the value at the given time by applying the given params
// using the given expression. If seq is nil, it will create a new sequence.
// It will also truncate the sequence based on the given truncateBefore.
func (seq Seq) Update(ts time.Time, params expr.Params, e expr.Expr, resolution time.Duration, truncateBefore time.Time) Seq {
	periodWidth := e.EncodedWidth()

	if log.IsTraceEnabled() {
		log.Tracef("Updating Seq starting at %v to %v at %v, truncating before %v", seq.Start().In(time.UTC), params, ts.In(time.UTC), truncateBefore.In(time.UTC))
	}

	if !ts.After(truncateBefore) {
		log.Trace("New value falls outside of truncation range, just truncate existing Seq")
		if len(seq) == 0 {
			return nil
		}
		return seq.Truncate(periodWidth, resolution, truncateBefore)
	}

	SeqEmpty := len(seq) == 0
	var start time.Time
	var gapPeriods int
	var maxPeriods int
	if !SeqEmpty {
		start = seq.Start()
		gapPeriods = int(ts.Sub(start) / resolution)
		maxPeriods = int(ts.Sub(truncateBefore) / resolution)
	}
	if SeqEmpty || start.Before(truncateBefore) || gapPeriods > maxPeriods {
		log.Trace("Creating new Seq")
		out := make(Seq, enc.Width64Bits+periodWidth)
		out.SetStart(ts)
		out.UpdateValueAt(0, e, params)
		return out
	}

	if ts.After(start) {
		log.Trace("Prepending to Seq")
		numPeriods := seq.NumPeriods(periodWidth) + gapPeriods
		origEnd := len(seq)
		if numPeriods > maxPeriods {
			log.Trace("Truncating existing Seq")
			numPeriods = maxPeriods
			origEnd = enc.Width64Bits + periodWidth*(numPeriods-gapPeriods)
		}
		out := New(periodWidth, numPeriods)
		copy(out[enc.Width64Bits+gapPeriods*periodWidth:], seq[enc.Width64Bits:origEnd])
		out.SetStart(ts)
		out.UpdateValueAt(0, e, params)
		return out
	}

	log.Trace("Updating existing entry on Seq")
	out := seq
	period := int(start.Sub(ts) / resolution)
	offset := period * periodWidth
	if offset+periodWidth >= len(seq) {
		// Grow seq
		out = make(Seq, offset+enc.Width64Bits+periodWidth)
		copy(out, seq)
	}
	out.UpdateValueAtOffset(offset, e, params)
	return out
}

// Merge merges the other sequence into this sequence.
func (seq Seq) Merge(other Seq, e expr.Expr, resolution time.Duration, truncateBefore time.Time) Seq {
	if seq == nil {
		return other
	}
	if other == nil {
		return seq
	}

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

	out := make(Seq, enc.Width64Bits+totalPeriods*encodedWidth)
	sout := out

	// Set start
	copy(sout, sa[:enc.Width64Bits])
	sout = sout[enc.Width64Bits:]
	sa = sa[enc.Width64Bits:]
	sb = sb[enc.Width64Bits:]

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

// Truncate truncates this sequence to the given given truncateBefore.
func (seq Seq) Truncate(periodWidth int, resolution time.Duration, truncateBefore time.Time) Seq {
	if seq == nil {
		return nil
	}
	maxPeriods := int(seq.Start().Sub(truncateBefore) / resolution)
	if maxPeriods <= 0 {
		// Entire Seq falls outside of truncation range
		return nil
	}
	maxLength := enc.Width64Bits + maxPeriods*periodWidth
	if maxLength >= len(seq) {
		return seq
	}
	return seq[:maxLength]
}

// String returns a string representation of this sequence using the given
// expression.
func (seq Seq) String(e expr.Expr) string {
	if seq == nil {
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
