package encoding

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRoundTimeUp(t *testing.T) {
	ti := time.Date(2015, 1, 1, 0, 0, 2, 0, time.UTC)
	to := time.Date(2015, 1, 1, 0, 1, 0, 0, time.UTC)
	assert.Equal(t, to, RoundTimeUp(ti, time.Minute))
}

func TestRoundTimeDown(t *testing.T) {
	ti := time.Date(2015, 1, 1, 0, 0, 2, 0, time.UTC)
	to := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, to, RoundTimeDown(ti, time.Minute))
}

func TestRoundTimeUntilUp(t *testing.T) {
	until := time.Date(2015, 1, 1, 0, 0, 1, 0, time.UTC)
	ti := time.Date(2015, 1, 1, 0, 0, 2, 0, time.UTC)
	to := time.Date(2015, 1, 1, 0, 1, 1, 0, time.UTC)
	assert.Equal(t, to, RoundTimeUntilUp(ti, time.Minute, until))
}

func TestRoundTimeUntilDown(t *testing.T) {
	until := time.Date(2015, 1, 1, 0, 0, 1, 0, time.UTC)
	ti := time.Date(2015, 1, 1, 0, 0, 2, 0, time.UTC)
	to := time.Date(2015, 1, 1, 0, 0, 1, 0, time.UTC)
	assert.Equal(t, to, RoundTimeUntilDown(ti, time.Minute, until))
}

func TestRoundTimeUntilUpZeroUntil(t *testing.T) {
	ti := time.Date(2015, 1, 1, 0, 0, 2, 0, time.UTC)
	to := RoundTimeUp(ti, time.Minute)
	assert.Equal(t, to, RoundTimeUntilUp(ti, time.Minute, time.Time{}))
}

func TestRoundTimeUntilDownZeroUntil(t *testing.T) {
	ti := time.Date(2015, 1, 1, 0, 0, 2, 0, time.UTC)
	to := RoundTimeDown(ti, time.Minute)
	assert.Equal(t, to, RoundTimeUntilDown(ti, time.Minute, time.Time{}))
}

func TestRoundTimeUntilUpZeroTime(t *testing.T) {
	until := time.Date(2015, 1, 1, 0, 0, 1, 0, time.UTC)
	ti := time.Time{}
	to := ti
	assert.Equal(t, to, RoundTimeUntilUp(ti, 13, until))
}

func TestRoundTimeUntilDownZeroTime(t *testing.T) {
	until := time.Date(2015, 1, 1, 0, 0, 1, 0, time.UTC)
	ti := time.Time{}
	to := ti
	assert.Equal(t, to, RoundTimeUntilDown(ti, 13, until))
}

func TestTimeFromInt(t *testing.T) {
	t1 := time.Date(2018, 1, 1, 1, 1, 1, 0, time.UTC)
	t2 := TimeFromInt(t1.UnixNano()).In(time.UTC)
	assert.Equal(t, t1, t2)
}

func TestTimeFromMillis(t *testing.T) {
	t1 := time.Date(2018, 1, 1, 1, 1, 1, 0, time.UTC)
	t2 := TimeFromMillis(t1.UnixNano() / nanosPerMilli).In(time.UTC)
	assert.Equal(t, t1, t2)
}
