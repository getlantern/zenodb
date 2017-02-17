package zenodb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
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

type walEntry struct {
	stream string
	data   []byte
	offset wal.Offset
}

type followSpec struct {
	followerID int
	offset     wal.Offset
}

type follower struct {
	common.Follow
	cb        func(data []byte, offset wal.Offset) error
	entries   chan *walEntry
	hasFailed int32
}

func (f *follower) read() {
	for entry := range f.entries {
		if f.failed() {
			continue
		}
		err := f.cb(entry.data, entry.offset)
		if err != nil {
			log.Errorf("Error on following for follower %d: %v", f.PartitionNumber, err)
			f.markFailed()
		}
	}
}

func (f *follower) submit(entry *walEntry) {
	f.entries <- entry
}

func (f *follower) markFailed() {
	atomic.StoreInt32(&f.hasFailed, 1)
}

func (f *follower) failed() bool {
	return atomic.LoadInt32(&f.hasFailed) == 1
}

func (db *DB) Follow(f *common.Follow, cb func([]byte, wal.Offset) error) {
	go db.processFollowersOnce.Do(db.processFollowers)
	fol := &follower{Follow: *f, cb: cb, entries: make(chan *walEntry, 10000)}
	db.followerJoined <- fol
	fol.read()
}

type partitionSpec struct {
	keys   []string
	tables map[string]map[int][]*followSpec
}

func (db *DB) processFollowers() {
	log.Debug("Starting to process followers")

	nextFollowerID := 0
	followers := make(map[int]*follower)
	streams := make(map[string]map[string]*partitionSpec)
	stopWALReaders := make(map[string]func())
	walEntries := make(chan *walEntry, 10000)
	h := partitionHash()
	includedFollowers := make([]int, 0, len(followers))

	stats := make([]int, db.opts.NumPartitions)
	statsInterval := 15 * time.Second
	statsTicker := time.NewTicker(statsInterval)

	newlyJoinedStreams := make(map[string]bool)
	onFollowerJoined := func(f *follower) {
		nextFollowerID++
		log.Debugf("Follower joined: %d -> %d", nextFollowerID, f.PartitionNumber)
		followers[nextFollowerID] = f

		partitions := streams[f.Stream]
		if partitions == nil {
			partitions = make(map[string]*partitionSpec)
			streams[f.Stream] = partitions
		}

		for _, partition := range f.Partitions {
			keys, sortedKeys := sortedPartitionKeys(partition.Keys)
			ps := partitions[keys]
			if ps == nil {
				ps = &partitionSpec{keys: sortedKeys, tables: make(map[string]map[int][]*followSpec)}
				partitions[keys] = ps
			}
			for _, t := range partition.Tables {
				table := ps.tables[t.Name]
				if table == nil {
					table = make(map[int][]*followSpec)
					ps.tables[t.Name] = table
				}
				specs := table[f.PartitionNumber]
				specs = append(specs, &followSpec{followerID: nextFollowerID, offset: t.Offset})
				table[f.PartitionNumber] = specs
			}
		}

		newlyJoinedStreams[f.Stream] = true
	}

	for {
		select {
		case f := <-db.followerJoined:
			// Clear out newlyJoinedStreams
			newlyJoinedStreams = make(map[string]bool)
			onFollowerJoined(f)
			// If more followers are waiting to join, grab them real quick
		extraFollowersLoop:
			for {
				select {
				case f := <-db.followerJoined:
					onFollowerJoined(f)
				default:
					break extraFollowersLoop
				}
			}

			for stream := range newlyJoinedStreams {
				var earliestOffset wal.Offset
				for _, partition := range streams[stream] {
					for _, table := range partition.tables {
						for _, specs := range table {
							for _, spec := range specs {
								if earliestOffset == nil || earliestOffset.After(spec.offset) {
									earliestOffset = spec.offset
								}
							}
						}
					}
				}

				stopWALReader := stopWALReaders[stream]
				if stopWALReader != nil {
					stopWALReader()
				}

				// Start following wal
				stopWALReader, err := db.followWAL(stream, earliestOffset, walEntries)
				if err != nil {
					log.Errorf("Unable to start following wal: %v", err)
					continue
				}
				stopWALReaders[stream] = stopWALReader
			}

		case entry := <-walEntries:
			data := entry.data
			offset := entry.offset
			// Skip timestamp
			_, remain := encoding.Read(data, encoding.Width64bits)
			dimsLen, remain := encoding.ReadInt32(remain)
			_dims, remain := encoding.Read(remain, dimsLen)
			dims := bytemap.ByteMap(_dims)

			partitions := streams[entry.stream]

			includedFollowers = includedFollowers[:0]
			for _, partition := range partitions {
				pid := db.partitionFor(h, dims, partition.keys)
				for tableName, table := range partition.tables {
					specs := table[pid]
					if len(specs) == 0 {
						continue
					}
					where := db.getTable(tableName).Where
					if where != nil && !where.Eval(dims).(bool) {
						continue
					}
					for _, spec := range specs {
						if offset.After(spec.offset) {
							includedFollowers = append(includedFollowers, spec.followerID)
							spec.offset = offset
						}
					}
				}
			}

			if len(includedFollowers) > 0 {
				sort.Ints(includedFollowers)
				lastIncluded := -1
				for _, included := range includedFollowers {
					if included == lastIncluded {
						// ignore duplicates
						continue
					}
					lastIncluded = included
					f := followers[included]
					if f.failed() {
						// ignore failed followers
						continue
					}
					f.submit(entry)
					stats[f.PartitionNumber]++
				}
			}

		case <-statsTicker.C:
			for partition, count := range stats {
				log.Debugf("Sent to follower %d: %v / s", partition, humanize.Comma(int64(float64(count)/statsInterval.Seconds())))
			}
			stats = make([]int, db.opts.NumPartitions)
		}
	}
}

func (db *DB) followWAL(stream string, offset wal.Offset, entries chan *walEntry) (func(), error) {
	var w *wal.WAL
	db.tablesMutex.RLock()
	w = db.streams[stream]
	db.tablesMutex.RUnlock()
	if w == nil {
		return nil, errors.New("Stream '%v' not found", stream)
	}

	log.Debugf("Following %v starting at %v", stream, offset)
	r, err := w.NewReader(fmt.Sprintf("clusterfollower.%v", stream), offset)
	if err != nil {
		return nil, errors.New("Unable to open wal reader for %v", stream)
	}

	stopped := int32(0)
	stop := make(chan bool, 1)
	finished := make(chan bool)
	go func() {
		defer func() {
			finished <- true
		}()

		for {
			data, err := r.Read()
			if err != nil {
				if atomic.LoadInt32(&stopped) == 1 {
					return
				}
				log.Debugf("Unable to read from stream '%v': %v", stream, err)
				continue
			}
			if data == nil {
				// Ignore empty data
				continue
			}
			dataCopy := make([]byte, len(data))
			copy(dataCopy, data)
			select {
			case entries <- &walEntry{stream: stream, data: dataCopy, offset: r.Offset()}:
				// okay
			case <-stop:
				return
			}
		}
	}()

	return func() {
		atomic.StoreInt32(&stopped, 1)
		stop <- true
		r.Close()
		<-finished
	}, nil
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
	return db.partitionFor(h, dims, partitionKeys) == partition
}

func (db *DB) partitionFor(h hash.Hash32, dims bytemap.ByteMap, partitionKeys []string) int {
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
	return int(h.Sum32()) % db.opts.NumPartitions
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
