package common

import (
	"github.com/getlantern/bytemap"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/encoding"
	"time"
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
