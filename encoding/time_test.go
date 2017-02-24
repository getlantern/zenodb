package encoding

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRoundTime(t *testing.T) {
	ti := time.Date(2015, 1, 1, 0, 0, 2, 0, time.UTC)
	to := time.Date(2015, 1, 1, 0, 1, 0, 0, time.UTC)
	assert.Equal(t, to, RoundTime(ti, time.Minute))
}

func TestRoundTimeUntil(t *testing.T) {
	until := time.Date(2015, 1, 1, 0, 0, 1, 0, time.UTC)
	ti := time.Date(2015, 1, 1, 0, 0, 2, 0, time.UTC)
	to := time.Date(2015, 1, 1, 0, 1, 1, 0, time.UTC)
	assert.Equal(t, to, RoundTimeUntil(ti, time.Minute, until))
}

func TestRoundTimeUntilZero(t *testing.T) {
	ti := time.Date(2015, 1, 1, 0, 0, 2, 0, time.UTC)
	to := ti
	assert.Equal(t, to, RoundTimeUntil(ti, time.Minute, time.Time{}))
}

func TestRoundTimeZero(t *testing.T) {
	until := time.Date(2015, 1, 1, 0, 0, 1, 0, time.UTC)
	ti := time.Time{}
	to := ti
	assert.Equal(t, to, RoundTimeUntil(ti, 13, until))
}
