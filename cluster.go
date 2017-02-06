package zenodb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/planner"
	"github.com/spaolacci/murmur3"
	"hash"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	keyIncludeMemStore = "zenodb.includeMemStore"
)

var (
	errCanceled = fmt.Errorf("following canceled")
)

func (db *DB) Follow(f *common.Follow, cb func([]byte, wal.Offset) error) error {
	var w *wal.WAL
	db.tablesMutex.RLock()
	w = db.streams[f.Stream]
	db.tablesMutex.RUnlock()
	if w == nil {
		return errors.New("Stream '%v' not found", f.Stream)
	}

	h := partitionHash()

	r, err := w.NewReader(fmt.Sprintf("follower.%d.%v", f.PartitionNumber, f.Stream), f.EarliestOffset)
	if err != nil {
		return errors.New("Unable to open wal reader for %v", f.Stream)
	}
	defer r.Close()

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
		offset := r.Offset()

		// See if we should include based on any of the combinations of partition
		// keys.
		include := func() bool {
			for _, partition := range f.Partitions {
				if !db.inPartition(h, dims, partition.Keys, f.PartitionNumber) {
					continue
				}
				for _, table := range partition.Tables {
					if table.Offset.After(offset) {
						continue
					}
					where := db.getTable(table.Name).Where
					if where == nil || where.Eval(dims).(bool) {
						return true
					}
				}
			}

			return false
		}

		if include() {
			err = cb(data, offset)
			if err != nil {
				log.Debugf("Unable to write to follower: %v", err)
				return err
			}
		}
	}
}

type tableWithOffset struct {
	t *table
	o wal.Offset
}

func (db *DB) followLeader(stream string, newSubscriber chan *tableWithOffset) {
	// Wait a little while for database to initialize
	timer := time.NewTimer(30 * time.Second)
	var tables []*table
	var offsets []wal.Offset
	partitions := make(map[string]*common.Partition)

waitForTables:
	for {
		select {
		case <-timer.C:
			if len(tables) == 0 {
				// Wait some more
				timer.Reset(5 * time.Second)
			}
			break waitForTables
		case subscriber := <-newSubscriber:
			table := subscriber.t
			offset := subscriber.o
			tables = append(tables, table)
			offsets = append(offsets, offset)
			partitionKeysString, partitionKeys := sortedPartitionKeys(table.PartitionBy)
			partition := partitions[partitionKeysString]
			if partition == nil {
				partition = &common.Partition{
					Keys: partitionKeys,
				}
				partitions[partitionKeysString] = partition
			}
			partition.Tables = append(partition.Tables, &common.PartitionTable{
				Name:   table.Name,
				Offset: offset,
			})
			// Got some tables, don't wait as long this time
			timer.Reset(1 * time.Second)
		}
	}

	for {
		cancel := make(chan bool, 100)
		go db.doFollowLeader(stream, tables, offsets, partitions, cancel)
		subscriber := <-newSubscriber
		cancel <- true
		tables = append(tables, subscriber.t)
		offsets = append(offsets, subscriber.o)
	}
}

func (db *DB) doFollowLeader(stream string, tables []*table, offsets []wal.Offset, partitions map[string]*common.Partition, cancel chan bool) {
	var earliestOffset wal.Offset
	for i, offset := range offsets {
		if i == 0 || earliestOffset.After(offset) {
			earliestOffset = offset
		}
	}

	if db.opts.MaxFollowAge > 0 {
		earliestAllowedOffset := wal.NewOffsetForTS(db.clock.Now().Add(-1 * db.opts.MaxFollowAge))
		if earliestAllowedOffset.After(earliestOffset) {
			log.Debugf("Forcibly limiting following to %v", earliestAllowedOffset)
			earliestOffset = earliestAllowedOffset
		}
	}

	log.Debugf("Following %v starting at %v", stream, earliestOffset)

	ins := make([]chan *walRead, 0, len(tables))
	for _, t := range tables {
		in := make(chan *walRead, 10000)
		ins = append(ins, in)
		go t.processInserts(in)
	}

	db.opts.Follow(&common.Follow{stream, earliestOffset, db.opts.Partition, partitions}, func(data []byte, newOffset wal.Offset) error {
		select {
		case <-cancel:
			// Canceled
			return errCanceled
		default:
			// Okay to continue
		}

		for i, in := range ins {
			priorOffset := offsets[i]
			if !priorOffset.After(newOffset) {
				in <- &walRead{data, newOffset}
				offsets[i] = newOffset
			}
		}
		return nil
	})
}

func sortedPartitionKeys(partitionKeys []string) (string, []string) {
	if len(partitionKeys) == 0 {
		return "", partitionKeys
	}
	sort.Strings(partitionKeys)
	return strings.Join(partitionKeys, "|"), partitionKeys
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
			b := dims.GetBytes(partitionKey)
			if len(b) > 0 {
				h.Write(b)
			}
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
