package zenodb

import (
	"fmt"
	"github.com/getlantern/bytemap"
)

type bytemapParams bytemap.ByteMap

func (bmp bytemapParams) Get(field string) (float64, bool) {
	// To support counting points, handle _point magic field specially
	if "_point" == field {
		return 1, true
	}
	result := bytemap.ByteMap(bmp).Get(field)
	if result == nil {
		return 0, false
	}
	return result.(float64), true
}

func (bmp bytemapParams) String() string {
	return fmt.Sprint(bytemap.ByteMap(bmp).AsMap())
}

type bytemapQueryParams bytemap.ByteMap

func (bmp bytemapQueryParams) Get(field string) (interface{}, error) {
	result := bytemap.ByteMap(bmp).Get(field)
	if result == nil {
		return "", nil
	}
	return result, nil
}

func (bmp bytemapQueryParams) String() string {
	return fmt.Sprint(bytemap.ByteMap(bmp).AsMap())
}
