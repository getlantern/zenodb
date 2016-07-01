package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestField(t *testing.T) {
	params := Map{
		"a": Float(4.4),
	}
	f := FIELD("a")
	assert.Equal(t, []string{"a"}, f.DependsOn())
	a := f.Accumulator()
	a.Update(params)
	assert.EqualValues(t, 4.4, a.Get())
}
