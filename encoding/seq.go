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
		maxPeriods := int(seq.Until().Sub(asOf) / resolution)
		if maxPeriods <= 0 {
			// Entire sequence falls outside of truncation range
			return nil
		}
		maxLength := Width64bits + maxPeriods*width
		if maxLength >= len(seq) {
			return seq
		}
		return seq[:maxLength]
	}

	return seq
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
