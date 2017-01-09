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
	"hash"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	keyIncludeMemStore = "zenodb.includeMemStore"

	defaultStream = "default"
)

type Follow struct {
	Stream    string
	Offset    wal.Offset
	Partition int
}

type QueryRemote func(sqlString string, includeMemStore bool, isSubQuery bool, subQueryResults [][]interface{}, onValue func(bytemap.ByteMap, []encoding.Sequence)) (hasReadResult bool, err error)

func (db *DB) Follow(f *Follow, cb func([]byte, wal.Offset) error) error {
	var w *wal.WAL
	db.tablesMutex.RLock()
	distinctPartitionKeys := db.distinctPartitionKeys
	w = db.streams[f.Stream]
	db.tablesMutex.RUnlock()
	if w == nil {
		return errors.New("Stream '%v' not found", f.Stream)
	}

	h := partitionHash()

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

		// See if we should include based on any of the combinations of partition
		// keys.
		include := false
		for _, partitionKeys := range distinctPartitionKeys {
			if db.inPartition(h, dims, partitionKeys, f.Partition) {
				include = true
				break
			}
		}
		if include {
			err = cb(data, r.Offset())
			if err != nil {
				log.Debugf("Unable to write to follower: %v", err)
				return err
			}
		}
	}
}

func partitionKeysToString(partitionKeys []string) string {
	if len(partitionKeys) == 0 {
		return defaultStream
	}
	return strings.Join(partitionKeys, "|")
}

func partitionHash() hash.Hash32 {
	// Use murmur hash for good key distribution
	return murmur3.New32()
}

func (db *DB) inPartition(h hash.Hash32, dims bytemap.ByteMap, partitionKeys []string, partition int) bool {
	h.Reset()
	if len(partitionKeys) > 0 {
		// Use specific partition keys
		for _, partitionKey := range partitionKeys {
			h.Write(dims.GetBytes(partitionKey))
		}
	} else {
		// Use all dims
		h.Write(dims)
	}
	return int(h.Sum32())%db.opts.NumPartitions == partition
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
				go handler(context.Background(), "", false, nil, false, nil, nil)
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

func (db *DB) queryForRemote(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
	source, err := db.Query(sqlString, isSubQuery, subQueryResults, shouldIncludeMemStore(ctx))
	if err != nil {
		return err
	}
	if unflat {
		return core.UnflattenOptimized(source).Iterate(ctx, onRow)
	} else {
		return source.Iterate(ctx, onFlatRow)
	}
}

func (db *DB) queryCluster(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, includeMemStore bool, unflat bool, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
	ctx = withIncludeMemStore(ctx, includeMemStore)
	numPartitions := db.opts.NumPartitions
	results := make(chan *remoteResult, numPartitions)
	resultsByPartition := make(map[int]*int64)
	timedOut := false
	var mx sync.Mutex

	subCtx := ctx
	ctxDeadline, ctxHasDeadline := subCtx.Deadline()
	if ctxHasDeadline {
		// Halve timeout for sub-contexts
		now := time.Now()
		timeout := ctxDeadline.Sub(now)
		var cancel context.CancelFunc
		subCtx, cancel = context.WithDeadline(subCtx, now.Add(timeout/2))
		defer cancel()
	}

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

				var newOnRow core.OnRow
				var newOnFlatRow core.OnFlatRow
				if unflat {
					newOnRow = func(key bytemap.ByteMap, vals core.Vals) (bool, error) {
						mx.Lock()
						if timedOut {
							mx.Unlock()
							return false, core.ErrDeadlineExceeded
						}
						atomic.AddInt64(resultsForPartition, 1)
						more, onRowErr := onRow(key, vals)
						mx.Unlock()
						return more, onRowErr
					}
				} else {
					newOnFlatRow = func(row *core.FlatRow) (bool, error) {
						mx.Lock()
						if timedOut {
							mx.Unlock()
							return false, core.ErrDeadlineExceeded
						}
						atomic.AddInt64(resultsForPartition, 1)
						more, onRowErr := onFlatRow(row)
						mx.Unlock()
						return more, onRowErr
					}
				}

				err := query(subCtx, sqlString, isSubQuery, subQueryResults, unflat, newOnRow, newOnFlatRow)
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
			if result.err != nil {
				log.Errorf("Error from partition %d: %v", result.partition, result.err)
				if finalErr == nil {
					finalErr = result.err
				}
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
