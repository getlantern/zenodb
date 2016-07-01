package tdb

import (
	"testing"

	. "github.com/oxtoacart/tdb/expr"
	"github.com/stretchr/testify/assert"
)

func TestSQL(t *testing.T) {
	aq := &Query{}
	_, err := aq.ApplySQL(`
SELECT AVG(a / (a + b + c)) AS rate
FROM table_a
WHERE dim_a =~ '^172.56.+' // this is a regex match
GROUP BY dim_a //, time('15s') // time is a special function
ORDER BY rate ASC
`)
	if assert.NoError(t, err) {
		if assert.Len(t, aq.fields, 1) {
			expected := ToString(AVG(DIV("a", ADD(ADD("a", "b"), "c"))))
			actual := ToString(aq.fields["rate"])
			assert.Equal(t, expected, actual)
		}
	}
}
