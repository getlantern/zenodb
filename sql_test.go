package tdb

import (
	"testing"
	"time"

	. "github.com/oxtoacart/tdb/expr"
	"github.com/stretchr/testify/assert"
)

func TestSQL(t *testing.T) {
	aq := &Query{}
	err := aq.applySQL(`
SELECT AVG(a / (A + b + C)) AS rate
FROM Table_A
WHERE Dim_a =~ '^172.56.+' // this is a regex match
GROUP BY dim_A, period(5s) // time is a special function
ORDER BY AVG(Rate) DESC
`)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, aq.fields, 1) {
		expected := ToString(AVG(DIV("a", ADD(ADD("a", "b"), "c"))))
		actual := ToString(aq.fields["rate"])
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, "table_a", aq.table)
	if assert.Len(t, aq.dims, 1) {
		assert.Equal(t, "dim_a", aq.dims[0])
	}
	if assert.Len(t, aq.orderBy, 1) {
		expected := ToString(MULT(-1, AVG("rate")))
		actual := ToString(aq.orderBy[0])
		assert.Equal(t, expected, actual)
	}
	assert.Equal(t, 5*time.Second, aq.resolution)
}
