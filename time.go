package tdb

import (
	"encoding/binary"
	"time"
)

func timeFromBytes(b []byte) time.Time {
	ts := int64(binary.BigEndian.Uint64(b))
	s := ts / int64(time.Second)
	ns := ts % int64(time.Second)
	return time.Unix(s, ns)
}
