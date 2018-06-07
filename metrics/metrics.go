package metrics

import (
	"sort"
	"sync"
	"time"

	"github.com/getlantern/wal"
)

var (
	leaderStats    *LeaderStats
	followerStats  map[int]*FollowerStats
	partitionStats map[int]*PartitionStats

	mx sync.RWMutex
)

func init() {
	reset()
}

func reset() {
	leaderStats = &LeaderStats{}
	followerStats = make(map[int]*FollowerStats, 0)
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
	followerID int
	Partition  int
	Queued     int
}

// PartitionStats provides stats for a single partition
type PartitionStats struct {
	Partition        int
	NumFollowers     int
	NumQueryHandlers int
}

type sortedFollowerStats []*FollowerStats

func (s sortedFollowerStats) Len() int      { return len(s) }
func (s sortedFollowerStats) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedFollowerStats) Less(i, j int) bool {
	if s[i].Partition < s[j].Partition {
		return true
	}
	return s[i].followerID < s[j].followerID
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
func FollowerJoined(followerID int, partition int) {
	mx.Lock()
	defer mx.Unlock()
	leaderStats.ConnectedFollowers++
	followerStats[followerID] = &FollowerStats{
		followerID: followerID,
		Partition:  partition,
		Queued:     0,
	}
	getPartitionStats(partition).NumFollowers++
}

// FollowerFailed records the fact that a follower failed (which is analogous to leaving)
func FollowerFailed(followerID int) {
	mx.Lock()
	defer mx.Unlock()
	// Only delete once
	fs, found := followerStats[followerID]
	if found {
		leaderStats.ConnectedFollowers--
		delete(followerStats, followerID)
		ps := getPartitionStats(fs.Partition)
		ps.NumFollowers--
		if ps.NumFollowers == 0 {
			leaderStats.ConnectedPartitions--
		}
	}
}

// QueryHandlerJoined reports when a query handler for a partition has joined
func QueryHandlerJoined(partition int) {
	mx.Lock()
	getPartitionStats(partition).NumQueryHandlers++
	mx.Unlock()
}

// QueryHandlerLeft reports when a query handler for a partition has left
func QueryHandlerLeft(partition int) {
	mx.Lock()
	getPartitionStats(partition).NumQueryHandlers--
	mx.Unlock()
}

// QueuedForFollower records how many measurements are queued for a given Follower
func QueuedForFollower(followerID int, queued int) {
	mx.Lock()
	defer mx.Unlock()
	followerStats[followerID].Queued = queued
}

func getPartitionStats(partition int) *PartitionStats {
	ps := partitionStats[partition]
	if ps == nil {
		ps = &PartitionStats{Partition: partition}
		partitionStats[partition] = ps
		leaderStats.ConnectedPartitions++
	}
	return ps
}

// GetStats returns stats for this cluster
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
	return s
}
