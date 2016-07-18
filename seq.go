package tdb

import (
	"fmt"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
)

// sequence represents a time-ordered sequence of accumulator states in
// descending time order. The first 8 bytes are the timestamp at which the
// sequence starts, and after that each n bytes are a floating point value for
// the next interval in the sequence, where n is determined by the type of
// accumulator.
type sequence []byte

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

func (seq sequence) valueAtTime(t time.Time, accum expr.Accumulator, resolution time.Duration) float64 {
	if seq == nil {
		return 0
	}
	start := seq.start()
	if t.After(start) {
		return 0
	}
	period := int(start.Sub(t) / resolution)
	return seq.valueAt(period, accum)
}

func (seq sequence) valueAt(period int, accum expr.Accumulator) float64 {
	if seq == nil {
		return 0
	}
	return seq.valueAtOffset(period*accum.EncodedWidth(), accum)
}

func (seq sequence) valueAtOffset(offset int, accum expr.Accumulator) float64 {
	if seq == nil {
		return 0
	}
	offset = offset + width64bits
	if offset >= len(seq) {
		return 0
	}
	accum.InitFrom(seq[offset:])
	return accum.Get()
}

func (seq sequence) updateValueAtTime(t time.Time, resolution time.Duration, accum expr.Accumulator, params expr.Params) {
	start := seq.start()
	period := int(start.Sub(t) / resolution)
	seq.updateValueAt(period, accum, params)
}

func (seq sequence) updateValueAt(period int, accum expr.Accumulator, params expr.Params) {
	seq.updateValueAtOffset(period*accum.EncodedWidth(), accum, params)
}

func (seq sequence) updateValueAtOffset(offset int, accum expr.Accumulator, params expr.Params) {
	offset = offset + width64bits
	s := seq[offset:]
	accum.InitFrom(s)
	accum.Update(params)
	accum.Encode(s)
}

func (seq sequence) update(tsp tsparams, accum expr.Accumulator, resolution time.Duration, truncateBefore time.Time) sequence {
	ts, params := tsp.timeAndParams()
	periodWidth := accum.EncodedWidth()

	if log.IsTraceEnabled() {
		log.Tracef("Updating sequence starting at %v to %v at %v, truncating before %v", seq.start().In(time.UTC), bytemap.ByteMap(bytemapParams(params)).AsMap(), ts.In(time.UTC), truncateBefore.In(time.UTC))
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
		out.updateValueAt(0, accum, params)
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
		out.updateValueAt(0, accum, params)
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
	out.updateValueAtOffset(offset, accum, params)
	return out.truncate(periodWidth, resolution, truncateBefore)
}

func (seq sequence) truncate(periodWidth int, resolution time.Duration, truncateBefore time.Time) sequence {
	if seq == nil {
		return nil
	}
	// New value falls outside of truncation range, truncate existing sequence
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

	accum := e.Accumulator()
	values := ""

	numPeriods := seq.numPeriods(accum.EncodedWidth())
	for i := 0; i < numPeriods; i++ {
		if i > 0 {
			values += " "
		}
		values += fmt.Sprint(seq.valueAt(i, accum))
	}
	return fmt.Sprintf("%v at %v: %v", e, seq.start(), values)
}
