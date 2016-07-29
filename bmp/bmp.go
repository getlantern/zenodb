package bmp

import (
	"fmt"

	"github.com/getlantern/bytemap"
)

type Params bytemap.ByteMap

func (bmp Params) Get(field string) (float64, bool) {
	result := bytemap.ByteMap(bmp).Get(field)
	if result == nil {
		return 0, false
	}
	return result.(float64), true
}

func (bmp Params) String() string {
	return fmt.Sprint(bytemap.ByteMap(bmp).AsMap())
}

type QueryParams bytemap.ByteMap

func (bmp QueryParams) Get(field string) (interface{}, error) {
	result := bytemap.ByteMap(bmp).Get(field)
	if result == nil {
		return "", nil
	}
	return result, nil
}

func (bmp QueryParams) String() string {
	return fmt.Sprint(bytemap.ByteMap(bmp).AsMap())
}
