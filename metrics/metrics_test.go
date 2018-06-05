package metrics

import (
	"testing"
	"time"

	"github.com/getlantern/wal"

	"github.com/stretchr/testify/assert"
)

func TestMetrics(t *testing.T) {
	reset()

	ts := time.Now()
	FollowerJoined(1, 1)
	FollowerJoined(2, 1)
	FollowerJoined(3, 2)
	FollowerJoined(4, 2)
	CurrentlyReadingWAL(wal.NewOffsetForTS(ts))
	QueuedForFollower(1, 11)
	QueuedForFollower(2, 22)
	QueuedForFollower(3, 33)
	QueuedForFollower(4, 44)

	s := GetStats()
	assert.Equal(t, 4, s.Leader.ConnectedFollowers)
	assert.Equal(t, ts.Format(time.RFC3339), s.Leader.CurrentlyReadingWAL)

	assert.Equal(t, 1, s.Followers[0].Partition)
	assert.Equal(t, 11, s.Followers[0].Queued)
	assert.Equal(t, 1, s.Followers[1].Partition)
	assert.Equal(t, 22, s.Followers[1].Queued)
	assert.Equal(t, 2, s.Followers[2].Partition)
	assert.Equal(t, 33, s.Followers[2].Queued)
	assert.Equal(t, 2, s.Followers[3].Partition)
	assert.Equal(t, 44, s.Followers[3].Queued)

	assert.Equal(t, 1, s.Partitions[0].Partition)
	assert.Equal(t, 2, s.Partitions[0].NumFollowers)
	assert.Equal(t, 2, s.Partitions[1].Partition)
	assert.Equal(t, 2, s.Partitions[1].NumFollowers)

	// Fail a couple of followers. Fail each twice to make sure we don't double-
	// subtract.
	FollowerFailed(2)
	FollowerFailed(2)
	FollowerFailed(3)
	FollowerFailed(3)

	s = GetStats()
	assert.Equal(t, 2, s.Leader.ConnectedFollowers)
	assert.Equal(t, 1, s.Followers[0].Partition)
	assert.Equal(t, 11, s.Followers[0].Queued)
	assert.Equal(t, 2, s.Followers[1].Partition)
	assert.Equal(t, 44, s.Followers[1].Queued)

	assert.Equal(t, 1, s.Partitions[0].Partition)
	assert.Equal(t, 1, s.Partitions[0].NumFollowers)
	assert.Equal(t, 2, s.Partitions[1].Partition)
	assert.Equal(t, 1, s.Partitions[1].NumFollowers)
}
