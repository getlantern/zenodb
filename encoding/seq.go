package encoding

import (
	"fmt"
	"math"
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
// with accumulator states of the given width.
func NewSequence(width int, numPeriods int) Sequence {
	return make(Sequence, Width64bits+numPeriods*width)
}

// NewFloatValue makes a single-value sequence from a simple expression,
// timestamp float value.
func NewFloatValue(e expr.Expr, ts time.Time, val float64) Sequence {
	return NewValue(e, ts, expr.FloatParams(val), nil)
}

// NewValue makes a single-value sequence from a simple expression, timestamp,
// params and metadata.
func NewValue(e expr.Expr, ts time.Time, params expr.Params, metadata goexpr.Params) Sequence {
	seq := NewSequence(e.EncodedWidth(), 1)
	seq.SetUntil(ts)
	seq.UpdateValueAt(0, e, params, metadata)
	return seq
}

// Until returns the most recent date represented by this Sequence.
func (seq Sequence) Until() time.Time {
	if len(seq) == 0 {
		return zeroTime
	}
	return TimeFromBytes(seq)
}

// Until returns the most recent date represented by this Sequence as an integer
func (seq Sequence) UntilInt() int64 {
	if len(seq) == 0 {
		return 0
	}
	return TimeIntFromBytes(seq)
}

// AsOf returns the oldest date represented by this Sequence.
func (seq Sequence) AsOf(width int, resolution time.Duration) time.Time {
	if len(seq) == 0 {
		return zeroTime
	}
	return seq.Until().Add(-1 * time.Duration(seq.NumPeriods(width)) * resolution)
}

// SetUntil sets the until time of this Sequence.
func (seq Sequence) SetUntil(t time.Time) {
	Binary.PutUint64(seq, uint64(t.UnixNano()))
}

// NumPeriods returns the number of periods in this Sequence assuming the given
// width.
func (seq Sequence) NumPeriods(width int) int {
	if len(seq) == 0 {
		return 0
	}
	return seq.DataLength() / width
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
	if e.IsConstant() {
		val, found, _ = e.Get(nil)
		return
	}
	if len(seq) == 0 {
		return 0, false
	}
	t = RoundTime(t, resolution)
	until := seq.Until()
	if t.After(until) {
		return 0, false
	}
	period := int(until.Sub(t) / resolution)
	return seq.ValueAt(period, e)
}

// ValueAt returns the value at the given period extracted using the given Expr.
// If no value is set for the given period, found will be false.
func (seq Sequence) ValueAt(period int, e expr.Expr) (val float64, found bool) {
	if e.IsConstant() {
		val, found, _ = e.Get(nil)
		return
	}
	if len(seq) == 0 {
		return 0, false
	}
	if period < 0 {
		return 0, false
	}
	return seq.ValueAtOffset(period*e.EncodedWidth(), e)
}

// ValueAtOffset returns the value at the given byte offset in the Sequence (not
// including the start time) extracted using the given Expr.  If no value is set
// for the given offset, found will be false.
func (seq Sequence) ValueAtOffset(offset int, e expr.Expr) (val float64, found bool) {
	if e.IsConstant() {
		val, found, _ = e.Get(nil)
		return
	}
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

// UpdateValueAt updates the value at the given period by applying the supplied
// Params to the given expression. metadata represents metadata about the
// operation that's used by the Expr as well (e.g. information about the
// dimensions associated to the value).
func (seq Sequence) UpdateValueAt(period int, e expr.Expr, params expr.Params, metadata goexpr.Params) {
	seq.UpdateValueAtOffset(period*e.EncodedWidth(), e, params, metadata)
}

// UpdateValueAtOffset updates the value at the given byte offset by applying
// the supplied Params to the given expression. metadata represents metadata
// about the operation that's used by the Expr as well (e.g. information about
// the dimensions associated to the value).
func (seq Sequence) UpdateValueAtOffset(offset int, e expr.Expr, params expr.Params, metadata goexpr.Params) {
	offset = offset + Width64bits
	e.Update(seq[offset:], params, metadata)
}

// Update unpacks the given TSParams and calls UpdateValue.
func (seq Sequence) Update(tsp TSParams, metadata goexpr.Params, e expr.Expr, resolution time.Duration, asOf time.Time) Sequence {
	ts, params := tsp.TimeAndParams()
	return seq.UpdateValue(ts, params, metadata, e, resolution, asOf)
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
func (seq Sequence) UpdateValue(ts time.Time, params expr.Params, metadata goexpr.Params, e expr.Expr, resolution time.Duration, asOf time.Time) Sequence {
	width := e.EncodedWidth()
	ts = RoundTime(ts, resolution)
	asOf = RoundTime(asOf, resolution)

	if log.IsTraceEnabled() {
		log.Tracef("Updating sequence starting at %v to %v at %v, truncating before %v", seq.Until().In(time.UTC), params, ts.In(time.UTC), asOf.In(time.UTC))
	}

	if !ts.After(asOf) {
		log.Trace("New value falls outside of truncation range, just truncate existing sequence")
		if len(seq) == 0 {
			return nil
		}
		return seq.Truncate(width, resolution, asOf, zeroTime)
	}

	sequenceEmpty := len(seq) == 0
	var start time.Time
	var gapPeriods int
	var maxPeriods int
	if !sequenceEmpty {
		start = seq.Until()
		gapPeriods = int(ts.Sub(start) / resolution)
		maxPeriods = int(ts.Sub(asOf) / resolution)
	}
	if sequenceEmpty || start.Before(asOf) || gapPeriods > maxPeriods {
		log.Trace("Creating new sequence")
		out := make(Sequence, Width64bits+width)
		out.SetUntil(ts)
		out.UpdateValueAt(0, e, params, metadata)
		return out
	}

	if ts.After(start) {
		log.Trace("Prepending to sequence")
		numPeriods := seq.NumPeriods(width) + gapPeriods
		origEnd := len(seq)
		if numPeriods > maxPeriods {
			log.Trace("Truncating existing sequence")
			numPeriods = maxPeriods
			origEnd = Width64bits + width*(numPeriods-gapPeriods)
		}
		out := NewSequence(width, numPeriods)
		copy(out[Width64bits+gapPeriods*width:], seq[Width64bits:origEnd])
		out.SetUntil(ts)
		out.UpdateValueAt(0, e, params, metadata)
		return out
	}

	log.Trace("Updating existing entry on sequence")
	out := seq
	period := int(start.Sub(ts) / resolution)
	offset := period * width
	if offset+width >= len(seq) {
		// Grow seq
		out = make(Sequence, offset+Width64bits+width)
		copy(out, seq)
	}
	out.UpdateValueAtOffset(offset, e, params, metadata)
	return out
}

func (seq Sequence) SubMerge(other Sequence, metadata goexpr.Params, resolution time.Duration, otherResolution time.Duration, ex expr.Expr, otherEx expr.Expr, submerge expr.SubMerge, asOf time.Time, until time.Time) (result Sequence) {
	result = seq
	otherWidth := otherEx.EncodedWidth()
	other = other.Truncate(otherWidth, otherResolution, asOf, until)
	otherPeriods := other.NumPeriods(otherWidth)
	if otherPeriods == 0 {
		return
	}

	width := ex.EncodedWidth()
	result = seq.Truncate(width, resolution, asOf, until)
	otherUntil := other.Until()
	newUntil := RoundTime(otherUntil, resolution)
	if len(result) <= Width64bits {
		result = NewSequence(width, 1)
		result.SetUntil(newUntil)
	}

	resultUntil := result.Until()
	periodsToPrepend := int(newUntil.Sub(resultUntil) / resolution)
	if periodsToPrepend > 0 {
		prepended := NewSequence(width, periodsToPrepend)
		prepended.SetUntil(newUntil)
		// Append existing data
		prepended = append(prepended, result[Width64bits:]...)
		result = prepended
		resultUntil = newUntil
	}

	otherAsOf := other.AsOf(otherWidth, otherResolution)
	newAsOf := RoundTime(otherAsOf, resolution)
	periodsToAppend := int(result.AsOf(width, resolution).Sub(newAsOf) / resolution)
	if periodsToAppend > 0 {
		appended := NewSequence(width, result.NumPeriods(width)+periodsToAppend)
		copy(appended, result)
		result = appended
	}

	// We assume that resolution is a positive integer multiple of otherResolution
	// (i.e. caller already checked this)
	scale := int(resolution / otherResolution)
	untilOffset := int(resultUntil.Sub(otherUntil) / otherResolution)
	resultPeriods := result.NumPeriods(width)
	for po := 0; po < otherPeriods; po++ {
		p := int(math.Floor(float64(po+untilOffset) / float64(scale)))
		if p >= resultPeriods {
			break
		}
		submerge(result[Width64bits+p*width:], other[Width64bits+po*otherWidth:], metadata)
	}
	return
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
	startA := sa.Until()
	startB := sb.Until()
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

// Truncate truncates all periods in the Sequence that fall outside of the given
// asOf and until.
func (seq Sequence) Truncate(width int, resolution time.Duration, asOf time.Time, until time.Time) (result Sequence) {
	if len(seq) == 0 {
		return nil
	}
	result = seq
	asOf = RoundTime(asOf, resolution)
	until = RoundTime(until, resolution)
	oldUntil := result.Until()

	if !until.IsZero() {
		periodsToRemove := int(oldUntil.Sub(until) / resolution)
		if periodsToRemove > 0 {
			bytesToRemove := periodsToRemove * width
			if bytesToRemove+Width64bits >= len(seq) {
				return nil
			}
			result = result[bytesToRemove:]
			result.SetUntil(until)
		}
	}

	if !asOf.IsZero() {
		maxPeriods := int(result.Until().Sub(asOf) / resolution)
		if maxPeriods <= 0 {
			// Entire sequence falls outside of truncation range
			return nil
		}
		maxLength := Width64bits + maxPeriods*width
		if maxLength >= len(result) {
			return result
		}
		return result[:maxLength]
	}

	return result
}

// String provides a string representation of this Sequence assuming that it
// holds data for the given Expr.
func (seq Sequence) String(e expr.Expr, resolution time.Duration) string {
	if len(seq) == 0 {
		return ""
	}

	values := ""

	numPeriods := seq.NumPeriods(e.EncodedWidth())
	for i := 0; i < numPeriods; i++ {
		if i > 0 {
			values += " "
		}
		val, _ := seq.ValueAt(numPeriods-1-i, e)
		values += fmt.Sprint(val)
	}
	return fmt.Sprintf("%v from %v -> %v: %v", e, seq.AsOf(e.EncodedWidth(), resolution).In(time.UTC), seq.Until().In(time.UTC), values)
}
