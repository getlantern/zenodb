package tdb

import (
	"github.com/getlantern/bytemap"
)

type bytemapParams bytemap.ByteMap

func (bmp bytemapParams) Get(field string) (float64, bool) {
	result := bytemap.ByteMap(bmp).Get(field)
	if result == nil {
		return 0, false
	}
	return result.(float64), true
}

type bytemapQueryParams bytemap.ByteMap

func (bmp bytemapQueryParams) Get(field string) (interface{}, error) {
	result := bytemap.ByteMap(bmp).Get(field)
	if result == nil {
		return "", nil
	}
	return result, nil
}
