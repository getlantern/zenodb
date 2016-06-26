package tdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRoundTime(t *testing.T) {
	ts := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	rounded := roundTime(ts, time.Second)
	expected := time.Date(2015, 5, 6, 7, 8, 9, 0, time.UTC)
	assert.Equal(t, expected, rounded)
}
