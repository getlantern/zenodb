package zenodb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/planner"
	"github.com/spaolacci/murmur3"
	"sync"
	"sync/atomic"
	"time"
)

const (
	keyIncludeMemStore = "zenodb.includeMemStore"
)

type Follow struct {
	Stream    string
	Offset    wal.Offset
	Partition int
}

type QueryRemote func(sqlString string, includeMemStore bool, isSubQuery bool, subQueryResults [][]interface{}, onValue func(bytemap.ByteMap, []encoding.Sequence)) (hasReadResult bool, err error)

func (db *DB) Follow(f *Follow, cb func([]byte, wal.Offset) error) error {
	db.tablesMutex.RLock()
	w := db.streams[f.Stream]
	db.tablesMutex.RUnlock()
	if w == nil {
		return errors.New("Stream '%v' not found", f.Stream)
	}

	// Use murmur hash for good key distribution
	h := murmur3.New32()

	r, err := w.NewReader(fmt.Sprintf("follower.%d.%v", f.Partition, f.Stream), f.Offset)
	if err != nil {
		return errors.New("Unable to open wal reader for %v", f.Stream)
	}
	for {
		data, err := r.Read()
		if err != nil {
			log.Debugf("Unable to read from stream '%v': %v", f.Stream, err)
			continue
		}
		if data == nil {
			// Ignore empty data
			continue
		}
		// Skip timestamp
		_, remain := encoding.Read(data, encoding.Width64bits)
		dimsLen, remain := encoding.ReadInt32(remain)
		_dims, remain := encoding.Read(remain, dimsLen)
		dims := bytemap.ByteMap(_dims)
		// Default to using all dims as the partition data
		partitionData := dims
		for _, dim := range db.opts.PartitionBy {
			candidate := dims.GetBytes(dim)
			if len(candidate) > 0 {
				// We found a specific dim, use it for partitioning
				partitionData = candidate
				break
			}
		}
		h.Reset()
		h.Write(partitionData)
		if int(h.Sum32())%db.opts.NumPartitions == f.Partition {
			err = cb(data, r.Offset())
			if err != nil {
				log.Debugf("Unable to write to follower: %v", err)
				return err
			}
		}
	}
}

func (db *DB) RegisterQueryHandler(partition int, query planner.QueryClusterFN) {
	db.tablesMutex.Lock()
	handlersCh := db.remoteQueryHandlers[partition]
	if handlersCh == nil {
		// TODO: maybe make size based on configuration or something
		handlersCh = make(chan planner.QueryClusterFN, 100)
	}
	db.remoteQueryHandlers[partition] = handlersCh
	db.tablesMutex.Unlock()
	handlersCh <- query
}

// freshenRemoteQueryHandlers periodically drains query handlers and sends noop
// queries in order to get fresh connections
func (db *DB) freshenRemoteQueryHandlers() {
	for {
		time.Sleep(5 * time.Minute)
		for i := 0; i < db.opts.NumPartitions; i++ {
			for {
				handler := db.remoteQueryHandlerForPartition(i)
				if handler == nil {
					break
				}
				go handler(context.Background(), "", false, nil, nil)
			}
		}
	}
}

func (db *DB) remoteQueryHandlerForPartition(partition int) planner.QueryClusterFN {
	db.tablesMutex.RLock()
	defer db.tablesMutex.RUnlock()
	select {
	case handler := <-db.remoteQueryHandlers[partition]:
		return handler
	default:
		return nil
	}
}

func withIncludeMemStore(ctx context.Context, includeMemStore bool) context.Context {
	return context.WithValue(ctx, keyIncludeMemStore, includeMemStore)
}

func shouldIncludeMemStore(ctx context.Context) bool {
	include := ctx.Value(keyIncludeMemStore)
	return include != nil && include.(bool)
}

func (db *DB) queryForRemote(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, onRow core.OnFlatRow) error {
	source, err := db.Query(sqlString, isSubQuery, subQueryResults, shouldIncludeMemStore(ctx))
	if err != nil {
		return err
	}
	return source.Iterate(ctx, onRow)
}

func (db *DB) queryCluster(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, includeMemStore bool, onRow core.OnFlatRow) error {
	ctx = withIncludeMemStore(ctx, includeMemStore)
	numPartitions := db.opts.NumPartitions
	results := make(chan *remoteResult, numPartitions)
	resultsByPartition := make(map[int]*int64)
	timedOut := false
	var mx sync.Mutex

	for i := 0; i < numPartitions; i++ {
		partition := i
		_resultsForPartition := int64(0)
		resultsForPartition := &_resultsForPartition
		resultsByPartition[partition] = resultsForPartition
		go func() {
			for {
				query := db.remoteQueryHandlerForPartition(partition)
				if query == nil {
					log.Errorf("No query handler for partition %d, ignoring", partition)
					results <- &remoteResult{partition, false, 0, nil}
					break
				}

				err := query(ctx, sqlString, isSubQuery, subQueryResults, func(row *core.FlatRow) (bool, error) {
					mx.Lock()
					if timedOut {
						mx.Unlock()
						return false, core.ErrDeadlineExceeded
					}
					atomic.AddInt64(resultsForPartition, 1)
					more, onRowErr := onRow(row)
					mx.Unlock()
					return more, onRowErr
				})

				if err != nil && atomic.LoadInt64(resultsForPartition) == 0 {
					log.Debugf("Failed on partition %d, haven't read anything, continuing: %v", partition, err)
					continue
				}
				results <- &remoteResult{partition, true, int(atomic.LoadInt64(resultsForPartition)), err}
				break
			}
		}()
	}

	start := time.Now()
	deadline := start.Add(10 * time.Minute)
	ctxDeadline, ctxHasDeadline := ctx.Deadline()
	if ctxHasDeadline {
		deadline = ctxDeadline
	}
	log.Debugf("Deadline for results from partitions: %v (T - %v)", deadline, deadline.Sub(time.Now()))

	timeout := time.NewTimer(deadline.Sub(time.Now()))
	resultCount := 0
	var finalErr error
	for i := 0; i < numPartitions; i++ {
		start := time.Now()
		select {
		case result := <-results:
			resultCount++
			if result.err != nil && finalErr == nil {
				finalErr = result.err
			}
			delta := time.Now().Sub(start)
			log.Debugf("%d/%d got %d results from partition %d in %v", resultCount, db.opts.NumPartitions, result.results, result.partition, delta)
			delete(resultsByPartition, result.partition)
		case <-timeout.C:
			mx.Lock()
			timedOut = true
			mx.Unlock()
			log.Errorf("Failed to get results by deadline, %d of %d partitions reporting", resultCount, numPartitions)
			msg := bytes.NewBuffer([]byte("Missing partitions: "))
			first := true
			for partition, results := range resultsByPartition {
				if !first {
					msg.WriteString(" | ")
				}
				first = false
				msg.WriteString(fmt.Sprintf("%d (%d)", partition, results))
			}
			log.Debug(msg.String())
			return finalErr
		}
	}

	return finalErr
}

type remoteResult struct {
	partition    int
	handlerFound bool
	results      int
	err          error
}
