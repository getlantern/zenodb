package tibsdb

import (
	"time"

	"github.com/getlantern/bytemap"
)

type tsparams []byte

func newTSParams(ts time.Time, params bytemap.ByteMap) tsparams {
	out := make([]byte, width64bits)
	encodeTime(out, ts)
	return tsparams(append(out, params...))
}

func (tsp tsparams) timeAndParams() (time.Time, bytemapParams) {
	ts := timeFromBytes(tsp)
	params := bytemapParams(tsp[width64bits:])
	return ts, params
}
