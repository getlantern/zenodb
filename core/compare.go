package core

import (
	"time"
)

func compare(a interface{}, b interface{}) int {
	if a == nil {
		if b != nil {
			return -1
		}
		return 0
	}
	if b == nil {
		if a != nil {
			return 1
		}
		return 0
	}

	switch ta := a.(type) {
	case bool:
		tvb := b.(bool)
		if ta && !tvb {
			return 1
		}
		if !ta && tvb {
			return -1
		}
	case byte:
		tvb := b.(byte)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case uint16:
		tvb := b.(uint16)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case uint32:
		tvb := b.(uint32)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case uint64:
		tvb := b.(uint64)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case uint:
		tvb := uint(b.(uint64))
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case int8:
		tvb := b.(int8)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case int16:
		tvb := b.(int16)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case int32:
		tvb := b.(int32)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case int64:
		tvb := b.(int64)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case int:
		tvb := b.(int)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case float32:
		tvb := b.(float32)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case float64:
		tvb := b.(float64)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case string:
		tvb := b.(string)
		if ta > tvb {
			return 1
		}
		if ta < tvb {
			return -1
		}
	case time.Time:
		tvb := b.(time.Time)
		if ta.After(tvb) {
			return 1
		}
		if ta.Before(tvb) {
			return -1
		}
	}

	return 0
}
