package metrics

import (
	"sort"
	"sync"
	"time"

	"github.com/getlantern/wal"
)

var (
	leaderStats   *LeaderStats
	followerStats map[int]*FollowerStats

	mx sync.RWMutex
)

func init() {
	reset()
}

func reset() {
	leaderStats = &LeaderStats{}
	followerStats = make(map[int]*FollowerStats, 0)
}

// Stats are the overall stats
type Stats struct {
	Leader    *LeaderStats
	Followers sortedFollowerStats
}

// LeaderStats provides stats for the cluster leader
type LeaderStats struct {
	ConnectedFollowers  int
	CurrentlyReadingWAL time.Time
}

// FollowerStats provides stats for a single Follower
type FollowerStats struct {
	followerId int
	Partition  int
	Queued     int
}

type sortedFollowerStats []*FollowerStats

func (s sortedFollowerStats) Len() int      { return len(s) }
func (s sortedFollowerStats) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedFollowerStats) Less(i, j int) bool {
	if s[i].Partition < s[j].Partition {
		return true
	}
	return s[i].followerId < s[j].followerId
}

func CurrentlyReadingWAL(offset wal.Offset) {
	ts := offset.TS()
	mx.Lock()
	leaderStats.CurrentlyReadingWAL = ts
	mx.Unlock()
}

// FollowerJoined records the fact that a follower joined the leader
func FollowerJoined(followerID int, partition int) {
	mx.Lock()
	defer mx.Unlock()
	leaderStats.ConnectedFollowers++
	followerStats[followerID] = &FollowerStats{
		followerId: followerID,
		Partition:  partition,
		Queued:     0,
	}
}

// FollowerFailed records the fact that a follower failed (which is analogous to leaving)
func FollowerFailed(followerID int) {
	mx.Lock()
	defer mx.Unlock()
	// Only delete once
	_, found := followerStats[followerID]
	if found {
		leaderStats.ConnectedFollowers--
		delete(followerStats, followerID)
	}
}

// QueuedForFollower records how many measurements are queued for a given Follower
func QueuedForFollower(followerID int, queued int) {
	mx.Lock()
	defer mx.Unlock()
	followerStats[followerID].Queued = queued
}

func GetStats() *Stats {
	mx.RLock()
	s := &Stats{
		Leader:    leaderStats,
		Followers: make(sortedFollowerStats, 0, len(followerStats)),
	}

	for _, followerStat := range followerStats {
		s.Followers = append(s.Followers, followerStat)
	}
	mx.RUnlock()

	sort.Sort(s.Followers)
	return s
}
