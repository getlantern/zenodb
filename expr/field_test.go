package expr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestField(t *testing.T) {
	params := Map{
		"a": 4.4,
	}
	f := FIELD("a")
	assert.Equal(t, []string{"a"}, f.DependsOn())
	b := make([]byte, f.EncodedWidth())
	_, val, _ := f.Update(b, params)
	assert.EqualValues(t, 4.4, val)
}
