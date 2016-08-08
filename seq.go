package zenodb

import (
	"fmt"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/getlantern/zenodb/expr"
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
	return seq.ValueAt(period, e)
}

func (seq sequence) ValueAt(period int, e expr.Expr) (float64, bool) {
	if seq == nil {
		return 0, false
	}
	if period < 0 {
		return 0, false
	}
	return seq.valueAtOffset(period*e.EncodedWidth(), e)
}

func (seq sequence) dataAt(period int, e expr.Expr) ([]byte, bool) {
	if seq == nil {
		return nil, false
	}
	if period < 0 {
		return nil, false
	}
	return seq.dataAtOffset(period*e.EncodedWidth(), e)
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

func (seq sequence) dataAtOffset(offset int, e expr.Expr) ([]byte, bool) {
	if seq == nil {
		return nil, false
	}
	offset = offset + width64bits
	if offset >= len(seq) {
		return nil, false
	}
	return seq[offset : offset+e.EncodedWidth()], true
}

func (seq sequence) updateValueAtTime(t time.Time, resolution time.Duration, e expr.Expr, params expr.Params, metadata govaluate.Parameters) {
	start := seq.start()
	period := int(start.Sub(t) / resolution)
	seq.updateValueAt(period, e, params, metadata)
}

func (seq sequence) updateValueAt(period int, e expr.Expr, params expr.Params, metadata govaluate.Parameters) {
	seq.updateValueAtOffset(period*e.EncodedWidth(), e, params, metadata)
}

func (seq sequence) mergeValueAt(period int, e expr.Expr, other []byte, metadata govaluate.Parameters) {
	seq.mergeValueAtOffset(period*e.EncodedWidth(), e, other, metadata)
}

func (seq sequence) subMergeValueAt(period int, e expr.Expr, subMerge expr.SubMerge, other []byte, metadata govaluate.Parameters) {
	seq.subMergeValueAtOffset(period*e.EncodedWidth(), e, subMerge, other, metadata)
}

func (seq sequence) updateValueAtOffset(offset int, e expr.Expr, params expr.Params, metadata govaluate.Parameters) {
	offset = offset + width64bits
	e.Update(seq[offset:], params, metadata)
}

func (seq sequence) mergeValueAtOffset(offset int, e expr.Expr, other []byte, metadata govaluate.Parameters) {
	offset = offset + width64bits
	orig := seq[offset:]
	e.Merge(orig, orig, other, metadata)
}

func (seq sequence) subMergeValueAtOffset(offset int, e expr.Expr, subMerge expr.SubMerge, other []byte, metadata govaluate.Parameters) {
	offset = offset + width64bits
	orig := seq[offset:]
	log.Debug(metadata)
	subMerge(orig, other, metadata)
}

func (seq sequence) update(tsp tsparams, metadata govaluate.Parameters, e expr.Expr, resolution time.Duration, truncateBefore time.Time) sequence {
	ts, params := tsp.timeAndParams()
	return seq.updateValue(ts, params, metadata, e, resolution, truncateBefore)
}

func (seq sequence) updateValue(ts time.Time, params expr.Params, metadata govaluate.Parameters, e expr.Expr, resolution time.Duration, truncateBefore time.Time) sequence {
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

	sequenceEmpty := len(seq) == 0
	var start time.Time
	var gapPeriods int
	var maxPeriods int
	if !sequenceEmpty {
		start = seq.start()
		gapPeriods = int(ts.Sub(start) / resolution)
		maxPeriods = int(ts.Sub(truncateBefore) / resolution)
	}
	if sequenceEmpty || start.Before(truncateBefore) || gapPeriods > maxPeriods {
		log.Trace("Creating new sequence")
		out := make(sequence, width64bits+periodWidth)
		out.setStart(ts)
		out.updateValueAt(0, e, params, metadata)
		return out
	}

	if ts.After(start) {
		log.Trace("Prepending to sequence")
		numPeriods := seq.numPeriods(periodWidth) + gapPeriods
		origEnd := len(seq)
		if numPeriods > maxPeriods {
			log.Trace("Truncating existing sequence")
			numPeriods = maxPeriods
			origEnd = width64bits + periodWidth*(numPeriods-gapPeriods)
		}
		out := newSequence(periodWidth, numPeriods)
		copy(out[width64bits+gapPeriods*periodWidth:], seq[width64bits:origEnd])
		out.setStart(ts)
		out.updateValueAt(0, e, params, metadata)
		return out
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
	out.updateValueAtOffset(offset, e, params, metadata)
	return out
}

func (seq sequence) merge(other sequence, e expr.Expr, resolution time.Duration, truncateBefore time.Time) sequence {
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

	if startB.Before(truncateBefore) {
		return sa
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
			sout, sa, sb = e.Merge(sout, sa, sb, nil)
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
		val, _ := seq.ValueAt(i, e)
		values += fmt.Sprint(val)
	}
	return fmt.Sprintf("%v at %v: %v", e, seq.start(), values)
}
