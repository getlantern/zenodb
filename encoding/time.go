package encoding

import (
	"time"
)

var (
	zeroTime = time.Time{}
)

func EncodeTime(b []byte, ts time.Time) {
	Binary.PutUint64(b, uint64(ts.UnixNano()))
}

func TimeFromBytes(b []byte) time.Time {
	ts := int64(Binary.Uint64(b))
	s := ts / int64(time.Second)
	ns := ts % int64(time.Second)
	return time.Unix(s, ns)
}

func RoundTime(ts time.Time, resolution time.Duration) time.Time {
	rounded := ts.Round(resolution)
	if rounded.After(ts) {
		rounded = rounded.Add(-1 * resolution)
	}
	return rounded
}
