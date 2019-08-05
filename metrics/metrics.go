package metrics

import (
	"sort"
	"sync"
	"time"

	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"
)

var (
	leaderStats    *LeaderStats
	followerStats  map[common.FollowerID]*FollowerStats
	partitionStats map[int]*PartitionStats

	mx sync.RWMutex
)

func init() {
	reset()
}

func reset() {
	leaderStats = &LeaderStats{}
	followerStats = make(map[common.FollowerID]*FollowerStats, 0)
	partitionStats = make(map[int]*PartitionStats, 0)
}

// Stats are the overall stats
type Stats struct {
	Leader     *LeaderStats
	Followers  sortedFollowerStats
	Partitions sortedPartitionStats
}

// LeaderStats provides stats for the cluster leader
type LeaderStats struct {
	NumPartitions       int
	ConnectedPartitions int
	ConnectedFollowers  int
	CurrentlyReadingWAL string
}

// FollowerStats provides stats for a single follower
type FollowerStats struct {
	FollowerID common.FollowerID
	Queued     int
}

// PartitionStats provides stats for a single partition
type PartitionStats struct {
	Partition    int
	NumFollowers int
}

type sortedFollowerStats []*FollowerStats

func (s sortedFollowerStats) Len() int      { return len(s) }
func (s sortedFollowerStats) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedFollowerStats) Less(i, j int) bool {
	return s[i].FollowerID.Partition < s[j].FollowerID.Partition || s[i].FollowerID.ID < s[j].FollowerID.ID
}

type sortedPartitionStats []*PartitionStats

func (s sortedPartitionStats) Len() int      { return len(s) }
func (s sortedPartitionStats) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedPartitionStats) Less(i, j int) bool {
	return s[i].Partition < s[j].Partition
}

// SetNumPartitions sets the number of partitions in the cluster
func SetNumPartitions(numPartitions int) {
	mx.Lock()
	leaderStats.NumPartitions = numPartitions
	mx.Unlock()
}

// CurrentlyReadingWAL indicates that we're currently reading the WAL at a given offset
func CurrentlyReadingWAL(offset wal.Offset) {
	ts := offset.TS()
	mx.Lock()
	leaderStats.CurrentlyReadingWAL = ts.Format(time.RFC3339)
	mx.Unlock()
}

// FollowerJoined records the fact that a follower joined the leader
func FollowerJoined(followerID common.FollowerID) {
	mx.Lock()
	defer mx.Unlock()
	getFollowerStats(followerID) // get follower stats to lazily initialize
	ps := partitionStats[followerID.Partition]
	if ps == nil {
		ps = &PartitionStats{Partition: followerID.Partition}
		partitionStats[followerID.Partition] = ps
	}
	ps.NumFollowers++
}

// FollowerFailed records the fact that a follower failed (which is analogous to leaving)
func FollowerFailed(followerID common.FollowerID) {
	mx.Lock()
	defer mx.Unlock()
	fs, found := followerStats[followerID]
	if found {
		delete(followerStats, followerID)
		partitionStats[fs.FollowerID.Partition].NumFollowers--
		if partitionStats[fs.FollowerID.Partition].NumFollowers == 0 {
			delete(partitionStats, fs.FollowerID.Partition)
		}
	}
}

// QueuedForFollower records how many measurements are queued for a given Follower
func QueuedForFollower(followerID common.FollowerID, queued int) {
	mx.Lock()
	defer mx.Unlock()
	fs, found := followerStats[followerID]
	if found {
		fs.Queued = queued
	}
}

func getFollowerStats(followerID common.FollowerID) *FollowerStats {
	fs, found := followerStats[followerID]
	if !found {
		fs = &FollowerStats{
			FollowerID: followerID,
			Queued:     0,
		}
		followerStats[followerID] = fs
	}
	return fs
}

func GetStats() *Stats {
	mx.RLock()
	s := &Stats{
		Leader:     leaderStats,
		Followers:  make(sortedFollowerStats, 0, len(followerStats)),
		Partitions: make(sortedPartitionStats, 0, len(partitionStats)),
	}

	for _, fs := range followerStats {
		s.Followers = append(s.Followers, fs)
	}
	for _, ps := range partitionStats {
		s.Partitions = append(s.Partitions, ps)
	}
	mx.RUnlock()

	sort.Sort(s.Followers)
	sort.Sort(s.Partitions)
	s.Leader.ConnectedPartitions = len(partitionStats)
	s.Leader.ConnectedFollowers = len(followerStats)
	return s
}
