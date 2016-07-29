package tdb

import (
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/bmp"
	"github.com/getlantern/tdb/enc"
	"github.com/getlantern/tdb/expr"
)

type tsparams []byte

func newTSParams(ts time.Time, params bytemap.ByteMap) tsparams {
	out := make([]byte, enc.Width64Bits)
	enc.Time(out, ts)
	return tsparams(append(out, params...))
}

func (tsp tsparams) timeAndParams() (time.Time, expr.Params) {
	ts := enc.TimeFromBytes(tsp)
	params := bmp.Params(tsp[enc.Width64Bits:])
	return ts, params
}
