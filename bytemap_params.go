package tdb

import (
	"github.com/getlantern/bytemap"
)

type bytemapParams bytemap.ByteMap

func (bmp bytemapParams) Get(field string) float64 {
	result := bytemap.ByteMap(bmp).Get(field)
	if result == nil {
		return 0
	}
	return result.(float64)
}

type bytemapQueryParams bytemap.ByteMap

func (bmp bytemapQueryParams) Get(field string) (interface{}, error) {
	result := bytemap.ByteMap(bmp).Get(field)
	if result == nil {
		return "", nil
	}
	return result, nil
}
