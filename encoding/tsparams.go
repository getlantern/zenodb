package encoding

import (
	"time"

	"github.com/getlantern/bytemap"
)

// TSParams combines a timestamp with a bytemap.ByteMap.
type TSParams []byte

func New(ts time.Time, params bytemap.ByteMap) TSParams {
	out := make([]byte, Width64bits)
	EncodeTime(out, ts)
	return TSParams(append(out, params...))
}

func (tsp TSParams) timeAndParams() (time.Time, bytemapParams) {
	ts := TimeFromBytes(tsp)
	params := bytemapParams(tsp[Width64bits:])
	return ts, params
}
