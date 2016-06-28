package calc

import (
	"github.com/oxtoacart/tdb/values"
)

type Calculated interface {
	Initial() values.Value
	Add(current values.Value, otherFields map[string]interface{}) (values.Value, error)
}
