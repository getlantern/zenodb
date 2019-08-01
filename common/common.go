package common

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/getlantern/bytemap"
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
	Name    string
	Offsets OffsetsBySource
}

type FollowerID struct {
	Partition int
	ID        int
}

func (id FollowerID) String() string {
	return fmt.Sprintf("%d.%d", id.Partition, id.ID)
}

type Follow struct {
	FollowerID     FollowerID
	Stream         string
	EarliestOffset wal.Offset
	Partitions     map[string]*Partition
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
	LowestHighWaterMark     int64
	HighestHighWaterMark    int64
	MissingPartitions       []int
}

// Retriable is a marker for retriable errors
type Retriable interface {
	error

	Retriable() bool
}

type retriable struct {
	wrapped error
}

func (err *retriable) Error() string {
	return fmt.Sprintf("%v (retriable)", err.wrapped.Error())
}

func (err *retriable) Retriable() bool {
	return true
}

// MarkRetriable marks the given error as retriable
func MarkRetriable(err error) Retriable {
	return &retriable{err}
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

// OffsetsBySource is a map of wal Offsets keyed to source ids
type OffsetsBySource map[int]wal.Offset

// Advance advances offsetsBySource to the higher of the current offset and the
// new offset from newOffsetsBySource, returning a New OffsetsBySource with the result.
func (offsetsBySource OffsetsBySource) Advance(newOffsetsBySource OffsetsBySource) OffsetsBySource {
	if offsetsBySource == nil {
		return newOffsetsBySource
	}
	if newOffsetsBySource == nil {
		return offsetsBySource
	}
	result := make(OffsetsBySource, len(offsetsBySource))
	for source, offset := range offsetsBySource {
		result[source] = offset
	}
	for source, newOffset := range newOffsetsBySource {
		oldOffset := result[source]
		if newOffset.After(oldOffset) {
			result[source] = newOffset
		}
	}
	return result
}

// LimitAge limits all offsets by source to be no earlier than the given limit
func (offsetsBySource OffsetsBySource) LimitAge(limit wal.Offset) OffsetsBySource {
	result := make(OffsetsBySource, len(offsetsBySource))
	for source, offset := range offsetsBySource {
		if limit.After(offset) {
			result[source] = limit
		} else {
			result[source] = offset
		}
	}
	return result
}

// LowestTS returns the lowest TS of any of the offsets
func (offsetsBySource OffsetsBySource) LowestTS() time.Time {
	var result time.Time
	for _, offset := range offsetsBySource {
		ts := offset.TS()
		if result.IsZero() || ts.Before(result) {
			result = ts
		}
	}
	return result
}

// HighestTS returns the highest TS of any of the offsets
func (offsetsBySource OffsetsBySource) HighestTS() time.Time {
	var result time.Time
	for _, offset := range offsetsBySource {
		ts := offset.TS()
		if result.IsZero() || ts.After(result) {
			result = ts
		}
	}
	return result
}

// TSString returns a string representation of the timestamps by source
func (offsetsBySource OffsetsBySource) TSString() string {
	var highWaterMarks []string
	for source, offset := range offsetsBySource {
		highWaterMarks = append(highWaterMarks, fmt.Sprintf("(%d)%v", source, offset.TS().Format(time.RFC3339)))
	}
	return strings.Join(highWaterMarks, " , ")
}

func (offsetsBySource OffsetsBySource) String() string {
	var highWaterMarks []string
	for source, offset := range offsetsBySource {
		highWaterMarks = append(highWaterMarks, fmt.Sprintf("(%d)%v", source, offset))
	}
	return strings.Join(highWaterMarks, " , ")
}
