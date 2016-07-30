package tibsdb

import (
	"time"
)

var (
	zeroTime = time.Time{}
)

func encodeTime(b []byte, ts time.Time) {
	binaryEncoding.PutUint64(b, uint64(ts.UnixNano()))
}

func timeFromBytes(b []byte) time.Time {
	ts := int64(binaryEncoding.Uint64(b))
	s := ts / int64(time.Second)
	ns := ts % int64(time.Second)
	return time.Unix(s, ns)
}

func roundTime(ts time.Time, resolution time.Duration) time.Time {
	rounded := ts.Round(resolution)
	if rounded.After(ts) {
		rounded = rounded.Add(-1 * resolution)
	}
	return rounded
}
