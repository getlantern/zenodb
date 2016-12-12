package encoding

import (
	"fmt"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/expr"
)

// TSParams combines a timestamp with a ByteMap.
type TSParams []byte

// NewTSParams constructs a new TSParams using the given Time and ByteMap.
func NewTSParams(ts time.Time, params bytemap.ByteMap) TSParams {
	out := make([]byte, Width64bits)
	EncodeTime(out, ts)
	return TSParams(append(out, params...))
}

// TimeAndParams returns the Time and Params components of this TSParams.
func (tsp TSParams) TimeAndParams() (time.Time, expr.Params) {
	ts := TimeFromBytes(tsp)
	params := bytemapParams(tsp[Width64bits:])
	return ts, params
}

// bytemapParams is an implementation of the expr.Params interface backed by a
// ByteMap.
type bytemapParams bytemap.ByteMap

func (bmp bytemapParams) Get(field string) (float64, bool) {
	var result interface{}
	result = bytemap.ByteMap(bmp).Get(field)
	if result == nil {
		// To support counting points, handle _point magic field specially
		if "_point" == field {
			result = bytemap.ByteMap(bmp).Get("_points")
			if result == nil {
				return 1, true
			}
		}
		return 0, false
	}
	return result.(float64), true
}

func (bmp bytemapParams) String() string {
	return fmt.Sprint(bytemap.ByteMap(bmp).AsMap())
}
