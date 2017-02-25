package encoding

import (
	"math"
	"time"
)

var (
	zeroTime = time.Time{}
)

func EncodeTime(b []byte, ts time.Time) {
	Binary.PutUint64(b, uint64(ts.UnixNano()))
}

func TimeFromBytes(b []byte) time.Time {
	return TimeFromInt(TimeIntFromBytes(b))
}

func TimeIntFromBytes(b []byte) int64 {
	return int64(Binary.Uint64(b))
}

func TimeFromInt(ts int64) time.Time {
	s := ts / int64(time.Second)
	ns := ts % int64(time.Second)
	return time.Unix(s, ns)
}

func RoundTimeUp(ts time.Time, resolution time.Duration) time.Time {
	rounded := ts.Round(resolution)
	if rounded.Before(ts) {
		rounded = rounded.Add(1 * resolution)
	}
	return rounded
}

func RoundTimeDown(ts time.Time, resolution time.Duration) time.Time {
	rounded := ts.Round(resolution)
	if rounded.After(ts) {
		rounded = rounded.Add(-1 * resolution)
	}
	return rounded
}

func RoundTimeUntilUp(ts time.Time, resolution time.Duration, until time.Time) time.Time {
	if ts.IsZero() {
		return ts
	}
	if until.IsZero() {
		return RoundTimeUp(ts, resolution)
	}
	delta := until.Sub(ts)
	periods := -1 * time.Duration(math.Floor(float64(delta)/float64(resolution)))
	return until.Add(periods * resolution)
}

func RoundTimeUntilDown(ts time.Time, resolution time.Duration, until time.Time) time.Time {
	if ts.IsZero() {
		return ts
	}
	if until.IsZero() {
		return RoundTimeDown(ts, resolution)
	}
	delta := until.Sub(ts)
	periods := -1 * time.Duration(math.Ceil(float64(delta)/float64(resolution)))
	return until.Add(periods * resolution)
}
