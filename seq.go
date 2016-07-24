package tdb

import (
	"fmt"
	"time"

	"github.com/getlantern/tdb/expr"
)

// sequence represents a time-ordered sequence of accumulator states in
// descending time order. The first 8 bytes are the timestamp at which the
// sequence starts, and after that each n bytes are a floating point value for
// the next interval in the sequence, where n is determined by the type of
// accumulator.
type sequence []byte

func newSequence(periodWidth int, numPeriods int) sequence {
	return make(sequence, width64bits+numPeriods*periodWidth)
}

func (seq sequence) start() time.Time {
	if seq == nil {
		return zeroTime
	}
	return timeFromBytes(seq)
}

func (seq sequence) setStart(t time.Time) {
	binaryEncoding.PutUint64(seq, uint64(t.UnixNano()))
}

func (seq sequence) numPeriods(periodWidth int) int {
	if seq == nil {
		return 0
	}
	return seq.dataLength() / periodWidth
}

// length without start time
func (seq sequence) dataLength() int {
	return len(seq) - width64bits
}

func (seq sequence) valueAtTime(t time.Time, e expr.Expr, resolution time.Duration) (float64, bool) {
	if seq == nil {
		return 0, false
	}
	start := seq.start()
	if t.After(start) {
		return 0, false
	}
	period := int(start.Sub(t) / resolution)
	return seq.valueAt(period, e)
}

func (seq sequence) valueAt(period int, e expr.Expr) (float64, bool) {
	if seq == nil {
		return 0, false
	}
	return seq.valueAtOffset(period*e.EncodedWidth(), e)
}

func (seq sequence) valueAtOffset(offset int, e expr.Expr) (float64, bool) {
	if seq == nil {
		return 0, false
	}
	offset = offset + width64bits
	if offset >= len(seq) {
		return 0, false
	}
	val, wasSet, _ := e.Get(seq[offset:])
	return val, wasSet
}

func (seq sequence) updateValueAtTime(t time.Time, resolution time.Duration, e expr.Expr, params expr.Params) {
	start := seq.start()
	period := int(start.Sub(t) / resolution)
	seq.updateValueAt(period, e, params)
}

func (seq sequence) updateValueAt(period int, e expr.Expr, params expr.Params) {
	seq.updateValueAtOffset(period*e.EncodedWidth(), e, params)
}

func (seq sequence) updateValueAtOffset(offset int, e expr.Expr, params expr.Params) {
	offset = offset + width64bits
	e.Update(seq[offset:], params)
}

func (seq sequence) update(tsp tsparams, e expr.Expr, resolution time.Duration, truncateBefore time.Time) sequence {
	ts, params := tsp.timeAndParams()
	return seq.updateValue(ts, params, e, resolution, truncateBefore)
}

func (seq sequence) updateValue(ts time.Time, params expr.Params, e expr.Expr, resolution time.Duration, truncateBefore time.Time) sequence {
	periodWidth := e.EncodedWidth()

	if log.IsTraceEnabled() {
		log.Tracef("Updating sequence starting at %v to %v at %v, truncating before %v", seq.start().In(time.UTC), params, ts.In(time.UTC), truncateBefore.In(time.UTC))
	}

	if !ts.After(truncateBefore) {
		log.Trace("New value falls outside of truncation range, just truncate existing sequence")
		if len(seq) == 0 {
			return nil
		}
		return seq.truncate(periodWidth, resolution, truncateBefore)
	}

	if len(seq) == 0 {
		log.Trace("Creating new sequence")
		out := make(sequence, width64bits+periodWidth)
		out.setStart(ts)
		out.updateValueAt(0, e, params)
		return out
	}

	start := seq.start()
	if ts.After(start) {
		log.Trace("Prepending to sequence")
		delta := ts.Sub(start)
		deltaPeriods := int(delta / resolution)
		out := make(sequence, len(seq)+periodWidth*deltaPeriods)
		copy(out[width64bits+deltaPeriods*periodWidth:], seq[width64bits:])
		out.setStart(ts)
		out.updateValueAt(0, e, params)
		// TODO: optimize this by applying truncation above
		return out.truncate(periodWidth, resolution, truncateBefore)
	}

	log.Trace("Updating existing entry on sequence")
	out := seq
	period := int(start.Sub(ts) / resolution)
	offset := period * periodWidth
	if offset+periodWidth >= len(seq) {
		// Grow seq
		out = make(sequence, offset+width64bits+periodWidth)
		copy(out, seq)
	}
	out.updateValueAtOffset(offset, e, params)
	return out.truncate(periodWidth, resolution, truncateBefore)
}

func (seq sequence) merge(other sequence, resolution time.Duration, e expr.Expr) sequence {
	if seq == nil {
		return other
	}
	if other == nil {
		return seq
	}

	sa := seq
	sb := other
	startA := sa.start()
	startB := sb.start()
	if startB.After(startA) {
		// Switch
		sa, startA, sb, startB = sb, startB, sa, startA
	}

	encodedWidth := e.EncodedWidth()
	aPeriods := sa.numPeriods(encodedWidth)
	bPeriods := sb.numPeriods(encodedWidth)
	endA := startA.Add(-1 * time.Duration(aPeriods) * resolution)
	endB := startB.Add(-1 * time.Duration(bPeriods) * resolution)
	end := endB
	if endA.Before(endB) {
		end = endA
	}
	totalPeriods := int(startA.Sub(end) / resolution)

	out := make(sequence, width64bits+totalPeriods*encodedWidth)
	sout := out

	// Set start
	copy(sout, sa[:width64bits])
	sout = sout[width64bits:]
	sa = sa[width64bits:]
	sb = sb[width64bits:]

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

func (seq sequence) truncate(periodWidth int, resolution time.Duration, truncateBefore time.Time) sequence {
	if seq == nil {
		return nil
	}
	maxPeriods := int(seq.start().Sub(truncateBefore) / resolution)
	if maxPeriods <= 0 {
		// Entire sequence falls outside of truncation range
		return nil
	}
	maxLength := width64bits + maxPeriods*periodWidth
	if maxLength >= len(seq) {
		return seq
	}
	return seq[:maxLength]
}

func (seq sequence) startingAt(periodWidth int, resolution time.Duration, start time.Time) sequence {
	if seq == nil {
		return nil
	}
	originalStart := seq.start()
	deltaPeriods := int(start.Sub(originalStart) / resolution)
	if deltaPeriods == 0 {
		return seq
	}
	numPeriods := seq.numPeriods(periodWidth) + deltaPeriods
	if numPeriods <= 0 {
		return nil
	}
	out := newSequence(periodWidth, numPeriods)
	sout := out
	sout.setStart(start)
	sout = sout[width64bits:]
	copy(sout, seq[len(seq)-numPeriods*periodWidth:])
	return out
}

func (seq sequence) String(e expr.Expr) string {
	if seq == nil {
		return ""
	}

	values := ""

	numPeriods := seq.numPeriods(e.EncodedWidth())
	for i := 0; i < numPeriods; i++ {
		if i > 0 {
			values += " "
		}
		values += fmt.Sprint(seq.valueAt(i, e))
	}
	return fmt.Sprintf("%v at %v: %v", e, seq.start(), values)
}
