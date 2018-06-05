package common

import (
	"context"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/msgpack"
	"github.com/getlantern/wal"

	"github.com/getlantern/zenodb/encoding"
)

const (
	keyIncludeMemStore = "zenodb.includeMemStore"

	nanosPerMilli = 1000000
)

type Partition struct {
	Keys   []string
	Tables []*PartitionTable
}

type PartitionTable struct {
	Name   string
	Offset wal.Offset
}

type Follow struct {
	Stream          string
	EarliestOffset  wal.Offset
	PartitionNumber int
	Partitions      map[string]*Partition
}

type QueryRemote func(sqlString string, includeMemStore bool, isSubQuery bool, subQueryResults [][]interface{}, onValue func(bytemap.ByteMap, []encoding.Sequence)) (hasReadResult bool, err error)

type QueryMetaData struct {
	FieldNames []string
	AsOf       time.Time
	Until      time.Time
	Resolution time.Duration
	Plan       string
}

// QueryStats captures stats about query
type QueryStats struct {
	NumPartitions           int
	NumSuccessfulPartitions int
	NumFailedPartitions     int
	LowestHighWaterMark     int64
	HighestHighWaterMark    int64
	Partitions              []*PartitionStats
}

// PartitionStats captures metadata about a specific partition's contribution
// to a query
type PartitionStats struct {
	HighWaterMark int64
	Error         string
}

func init() {
	msgpack.RegisterExt(10, &QueryStats{})
	msgpack.RegisterExt(11, &PartitionStats{})
}

func WithIncludeMemStore(ctx context.Context, includeMemStore bool) context.Context {
	return context.WithValue(ctx, keyIncludeMemStore, includeMemStore)
}

func ShouldIncludeMemStore(ctx context.Context) bool {
	include := ctx.Value(keyIncludeMemStore)
	return include != nil && include.(bool)
}

func NanosToMillis(nanos int64) int64 {
	return nanos / nanosPerMilli
}

func TimeToMillis(ts time.Time) int64 {
	return NanosToMillis(ts.UnixNano())
}
