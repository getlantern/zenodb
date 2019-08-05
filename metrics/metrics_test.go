package metrics

import (
	"testing"
	"time"

	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"

	"github.com/stretchr/testify/assert"
)

func TestMetrics(t *testing.T) {
	reset()

	ts := time.Now()
	FollowerJoined(common.FollowerID{1, 1})
	FollowerJoined(common.FollowerID{1, 2})
	FollowerJoined(common.FollowerID{2, 3})
	FollowerJoined(common.FollowerID{2, 4})
	CurrentlyReadingWAL(wal.NewOffsetForTS(ts))
	QueuedForFollower(common.FollowerID{1, 1}, 11)
	QueuedForFollower(common.FollowerID{1, 2}, 22)
	QueuedForFollower(common.FollowerID{2, 3}, 33)
	QueuedForFollower(common.FollowerID{2, 4}, 44)

	s := GetStats()
	assert.Equal(t, 4, s.Leader.ConnectedFollowers)
	assert.Equal(t, ts.Format(time.RFC3339), s.Leader.CurrentlyReadingWAL)

	assert.Equal(t, 1, s.Followers[0].FollowerID.Partition)
	assert.Equal(t, 11, s.Followers[0].Queued)
	assert.Equal(t, 1, s.Followers[1].FollowerID.Partition)
	assert.Equal(t, 22, s.Followers[1].Queued)
	assert.Equal(t, 2, s.Followers[2].FollowerID.Partition)
	assert.Equal(t, 33, s.Followers[2].Queued)
	assert.Equal(t, 2, s.Followers[3].FollowerID.Partition)
	assert.Equal(t, 44, s.Followers[3].Queued)

	assert.Equal(t, 2, s.Leader.ConnectedPartitions)
	assert.Equal(t, 1, s.Partitions[0].Partition)
	assert.Equal(t, 2, s.Partitions[0].NumFollowers)
	assert.Equal(t, 2, s.Partitions[1].Partition)
	assert.Equal(t, 2, s.Partitions[1].NumFollowers)

	// Fail a couple of followers. Fail each twice to make sure we don't double-
	// subtract.
	FollowerFailed(common.FollowerID{1, 2})
	FollowerFailed(common.FollowerID{1, 2})
	FollowerFailed(common.FollowerID{2, 3})
	FollowerFailed(common.FollowerID{2, 3})

	s = GetStats()
	assert.Equal(t, 2, s.Leader.ConnectedFollowers)
	assert.Equal(t, 2, s.Leader.ConnectedPartitions)
	assert.Equal(t, 1, s.Partitions[0].Partition)
	assert.Equal(t, 1, s.Partitions[0].NumFollowers)
	assert.Equal(t, 2, s.Partitions[1].Partition)
	assert.Equal(t, 1, s.Partitions[1].NumFollowers)

	// Fail remaining followers.
	FollowerFailed(common.FollowerID{1, 1})
	FollowerFailed(common.FollowerID{1, 1})
	FollowerFailed(common.FollowerID{2, 4})
	FollowerFailed(common.FollowerID{2, 4})

	s = GetStats()
	assert.Zero(t, s.Leader.ConnectedFollowers)
	assert.Zero(t, s.Leader.ConnectedPartitions)

	// Re-join
	FollowerJoined(common.FollowerID{1, 1})
	FollowerJoined(common.FollowerID{1, 2})

	s = GetStats()
	assert.Equal(t, 2, s.Leader.ConnectedFollowers)
	assert.Equal(t, 1, s.Leader.ConnectedPartitions)
}
