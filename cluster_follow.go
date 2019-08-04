package zenodb

import (
	"fmt"
	"hash"
	"io"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/metrics"
	"github.com/spaolacci/murmur3"
)

var (
	errCanceled = fmt.Errorf("following canceled")
	errStopped  = fmt.Errorf("database stopped")
)

type walEntry struct {
	stream string
	data   []byte
	offset wal.Offset
}

type followSpec struct {
	followerID common.FollowerID
	offset     wal.Offset
}

type follower struct {
	common.Follow
	db        *DB
	cb        func(data []byte, offset wal.Offset) error
	entries   chan *walEntry
	hasFailed int32
}

func (f *follower) read() {
	for entry := range f.entries {
		if f.failed() {
			continue
		}
		// TODO: don't hardcode this
		if len(entry.data) > 2000000 {
			f.db.log.Debugf("Discarding entry greater than 2 MB")
			continue
		}
		err := f.cb(entry.data, entry.offset)
		if err != nil {
			f.db.log.Errorf("Error on following for follower %d: %v", f.FollowerID.Partition, err)
			f.markFailed()
		}
	}
}

func (f *follower) submit(entry *walEntry) {
	if f.failed() {
		close(f.entries)
		return
	}
	f.entries <- entry
}

func (f *follower) markFailed() {
	atomic.StoreInt32(&f.hasFailed, 1)
	metrics.FollowerFailed(f.FollowerID)
}

func (f *follower) failed() bool {
	return atomic.LoadInt32(&f.hasFailed) == 1
}

func (db *DB) Follow(f *common.Follow, cb func([]byte, wal.Offset) error) {
	db.Go(func(stop <-chan interface{}) {
		db.processFollowersOnce.Do(func() {
			db.processFollowers(stop)
		})
	})
	fol := &follower{Follow: *f, db: db, cb: cb, entries: make(chan *walEntry, 1000000)} // TODO: make this buffer tunable
	db.followerJoined <- fol
	fol.read()
}

type tableSpec struct {
	where                goexpr.Expr
	whereString          string
	followersByPartition map[int]map[common.FollowerID]*followSpec
}

type partitionSpec struct {
	keys   []string
	tables map[string]*tableSpec
}

func (db *DB) processFollowers(stop <-chan interface{}) {
	db.log.Debug("Starting to process followers")

	followers := make(map[common.FollowerID]*follower)
	streams := make(map[string]map[string]*partitionSpec)
	stopWALReaders := make(map[string]func())
	includedFollowers := make([]common.FollowerID, 0, len(followers))

	stats := make([]int, db.opts.NumPartitions)
	statsInterval := 1 * time.Minute
	statsTicker := time.NewTicker(statsInterval)

	newlyJoinedStreams := make(map[string]bool)
	onFollowerJoined := func(f *follower) {
		metrics.FollowerJoined(f.FollowerID)
		db.log.Debugf("Follower %v joined starting at offset %v", f.FollowerID, f.EarliestOffset)
		followers[f.FollowerID] = f

		partitions := streams[f.Stream]
		if partitions == nil {
			partitions = make(map[string]*partitionSpec)
			streams[f.Stream] = partitions
		}

		for _, partition := range f.Partitions {
			keys, sortedKeys := sortedPartitionKeys(partition.Keys)
			ps := partitions[keys]
			if ps == nil {
				ps = &partitionSpec{keys: sortedKeys, tables: make(map[string]*tableSpec)}
				partitions[keys] = ps
			}
			for _, t := range partition.Tables {
				table := ps.tables[t.Name]
				if table == nil {
					tb := db.getTable(t.Name)
					if tb == nil {
						db.log.Errorf("Table %v requested by %v not found, not including from WAL", t.Name, f.FollowerID)
						continue
					}
					where := tb.Where
					whereString := ""
					if where != nil {
						whereString = strings.ToLower(where.String())
					}
					table = &tableSpec{
						where:                where,
						whereString:          whereString,
						followersByPartition: make(map[int]map[common.FollowerID]*followSpec),
					}
					ps.tables[t.Name] = table
				}
				specs := table.followersByPartition[f.FollowerID.Partition]
				if specs == nil {
					specs = make(map[common.FollowerID]*followSpec)
					table.followersByPartition[f.FollowerID.Partition] = specs
				}
				offset := t.Offsets[db.opts.ID]
				if offset.After(f.EarliestOffset) {
					offset = f.EarliestOffset
				}
				spec := &followSpec{followerID: f.FollowerID, offset: offset}
				specs[f.FollowerID] = spec
				db.log.Debugf("%v following %v starting at %v", f.FollowerID, t.Name, f.EarliestOffset)
			}
		}

		newlyJoinedStreams[f.Stream] = true
	}

	defer func() {
		// Stop all WAL readers when we stop processing followers
		for _, stop := range stopWALReaders {
			stop()
		}
	}()

	var requests chan *partitionRequest
	var results chan *partitionsResult

	printStats := func() {
		for partition, count := range stats {
			if count > 0 {
				db.log.Debugf("Sent to follower %d: %d at %v / s", partition, count, humanize.Comma(int64(float64(count)/statsInterval.Seconds())))
			}
		}
		stats = make([]int, db.opts.NumPartitions)

		for _, f := range followers {
			queued := int64(len(f.entries))
			metrics.QueuedForFollower(f.FollowerID, int(queued))
			if queued > 0 {
				db.log.Debugf("Queued for follower %v: %v", f.FollowerID, humanize.Comma(queued))
			}
		}
	}
	defer printStats()

	for {
		select {
		case <-stop:
			return
		case f := <-db.followerJoined:
			printStats()

			// Make a copy of streams to avoid modifying old ones
			streamsCopy := make(map[string]map[string]*partitionSpec, len(streams))
			for stream, partitions := range streams {
				partitionsCopy := make(map[string]*partitionSpec, len(partitions))
				streamsCopy[stream] = partitionsCopy
				for partitionKey, partition := range partitions {
					partitionCopy := &partitionSpec{
						keys:   partition.keys,
						tables: make(map[string]*tableSpec, len(partition.tables)),
					}
					partitionsCopy[partitionKey] = partitionCopy
					for tableName, table := range partition.tables {
						tableCopy := &tableSpec{
							where:                table.where,
							whereString:          table.whereString,
							followersByPartition: make(map[int]map[common.FollowerID]*followSpec, len(table.followersByPartition)),
						}
						partitionCopy.tables[tableName] = tableCopy
						for key, specs := range table.followersByPartition {
							specsCopy := make(map[common.FollowerID]*followSpec, len(specs))
							for followerID, spec := range specs {
								specsCopy[followerID] = spec
							}
							tableCopy.followersByPartition[key] = specsCopy
						}
					}
				}
			}
			streams = streamsCopy

			oldRequests := requests
			db.log.Debug("Starting parallel entry processing")
			requests, results = db.startParallelEntryProcessing()

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
				hasOffset := false
				for _, partition := range streams[stream] {
					for _, table := range partition.tables {
						for _, specs := range table.followersByPartition {
							for followerID, spec := range specs {
								db.log.Debugf("Offset on %v for %v is %v", stream, followerID, spec.offset)
								if !hasOffset {
									db.log.Debugf("Follower %v has first offset on %v, using it: %v", followerID, stream, spec.offset)
									hasOffset = true
								} else if earliestOffset.After(spec.offset) {
									db.log.Debugf("Follower %v has earlier offset on %v than %v, using it: %v", followerID, stream, earliestOffset, spec.offset)
									earliestOffset = spec.offset
								}
							}
						}
					}
				}

				stopWALReader := stopWALReaders[stream]
				if stopWALReader != nil {
					db.log.Debugf("Stopping WAL reader for %v", stream)
					stopWALReader()
				}

				db.log.Debugf("Start following WAL for %v", stream)
				stopWALReader, err := db.followWAL(stream, earliestOffset, streams[stream], requests)
				if err != nil {
					db.log.Errorf("Unable to start following wal: %v", err)
					continue
				}
				stopWALReaders[stream] = stopWALReader

				if oldRequests != nil {
					close(oldRequests)
				}
			}

		case result, ok := <-results:
			if !ok {
				return
			}

			entry := result.entry
			partitions := streams[entry.stream]
			offset := entry.offset

			includedFollowers = includedFollowers[:0]
			for partitionKeys, partition := range partitions {
				pr := result.partitions[partitionKeys]
				pid := pr.pid
				for tableName, table := range partition.tables {
					specs := table.followersByPartition[pid]
					if len(specs) == 0 {
						continue
					}
					wherePassed := pr.wherePassed[tableName]
					if wherePassed {
						for _, spec := range specs {
							if offset.After(spec.offset) {
								includedFollowers = append(includedFollowers, spec.followerID)
							}
						}
					}
					// Update offset for all specs
					for _, spec := range specs {
						if offset.After(spec.offset) {
							spec.offset = offset
						}
					}
				}
			}

			for _, included := range includedFollowers {
				f := followers[included]
				if f.failed() {
					// ignore failed followers
					continue
				}
				f.submit(entry)
				stats[f.FollowerID.Partition]++
			}

		case <-statsTicker.C:
			printStats()
		}
	}
}

type partitionRequest struct {
	partitions map[string]*partitionSpec
	entry      *walEntry
}

type partitionsResult struct {
	entry      *walEntry
	partitions map[string]*partitionResult
}

type partitionResult struct {
	pid         int
	wherePassed map[string]bool
}

type partitionsResultsByOffset []*partitionsResult

func (r partitionsResultsByOffset) Len() int      { return len(r) }
func (r partitionsResultsByOffset) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r partitionsResultsByOffset) Less(i, j int) bool {
	return r[j].entry.offset.After(r[i].entry.offset)
}

func (db *DB) startParallelEntryProcessing() (chan *partitionRequest, chan *partitionsResult) {
	// Use up to all of our CPU capacity - 1 for doing this processing
	parallelism := runtime.NumCPU() - 1
	if parallelism < 1 {
		parallelism = 1
	}
	db.log.Debugf("Using %d CPUs to process entries for followers", parallelism)

	requests := make(chan *partitionRequest, parallelism*db.opts.NumPartitions*10) // TODO: make this tunable
	in := make(chan *partitionRequest, parallelism*db.opts.NumPartitions*10)
	mapped := make(chan *partitionsResult, parallelism*db.opts.NumPartitions*10)
	results := make(chan *partitionsResult, parallelism*db.opts.NumPartitions*10)
	queued := make(chan int)
	drained := make(chan bool)

	db.Go(func(stop <-chan interface{}) {
		db.enqueuePartitionRequests(parallelism, requests, in, queued, drained, stop)
	})
	for i := 0; i < parallelism; i++ {
		db.Go(func(stop <-chan interface{}) {
			db.mapPartitionRequests(in, mapped, stop)
		})
	}
	db.Go(func(stop <-chan interface{}) {
		db.reducePartitionRequests(parallelism, mapped, results, queued, drained, stop)
	})

	return requests, results
}

func (db *DB) enqueuePartitionRequests(parallelism int, requests chan *partitionRequest, in chan *partitionRequest, queued chan int, drained chan bool, stop <-chan interface{}) {
	q := 0
	markQueued := func() {
		if q > 0 {
			select {
			case <-stop:
			case queued <- q:
				select {
				case <-stop:
				case <-drained:
					q = 0
				}
			}
		}
	}

	defer func() {
		markQueued()
		close(in)
		close(queued)
	}()

	for {
		select {
		case <-stop:
			return
		case req, more := <-requests:
			if req != nil {
				in <- req
				q++
				if q == parallelism {
					markQueued()
				}
			}
			if !more {
				return
			}
		default:
			markQueued()
			time.Sleep(1 * time.Second)
		}
	}
}

func (db *DB) mapPartitionRequests(in chan *partitionRequest, mapped chan *partitionsResult, stop <-chan interface{}) {
	h := partitionHash()
	for {
		select {
		case <-stop:
			return
		case req, ok := <-in:
			if !ok {
				return
			}
			db.mapPartitionRequest(h, req, mapped)
		}
	}
}

func (db *DB) mapPartitionRequest(h hash.Hash32, req *partitionRequest, mapped chan *partitionsResult) {
	defer func() {
		p := recover()
		if p != nil {
			db.log.Errorf("Panic in following: %v", p)
		}
	}()

	partitions := req.partitions
	entry := req.entry
	result := &partitionsResult{
		entry:      entry,
		partitions: make(map[string]*partitionResult),
	}

	data := entry.data
	// Skip timestamp
	_, remain := encoding.Read(data, encoding.Width64bits)
	dimsLen, remain := encoding.ReadInt32(remain)
	_dims, _ := encoding.Read(remain, dimsLen)
	dims := bytemap.ByteMap(_dims)

	whereResults := make(map[string]bool, 50)

	for partitionKeys, partition := range partitions {
		pid := db.partitionFor(h, dims, partition.keys)
		pr := &partitionResult{pid: pid, wherePassed: make(map[string]bool, len(partition.tables))}
		result.partitions[partitionKeys] = pr
		for tableName, table := range partition.tables {
			specs := table.followersByPartition[pid]
			if len(specs) == 0 {
				continue
			}
			wherePassed, found := whereResults[table.whereString]
			if !found {
				wherePassed = table.where == nil || table.where.Eval(dims).(bool)
				whereResults[table.whereString] = wherePassed
			}
			pr.wherePassed[tableName] = wherePassed
		}
	}

	mapped <- result
}

func (db *DB) reducePartitionRequests(parallelism int, mapped chan *partitionsResult, results chan *partitionsResult, queued chan int, drained chan bool, stop <-chan interface{}) {
	defer close(results)
	buf := make(partitionsResultsByOffset, 0, parallelism)
	for {
		select {
		case <-stop:
			return
		case numQueued := <-queued:
			buf = buf[:0]
			for q := 0; q < numQueued; q++ {
				buf = append(buf, <-mapped)
			}
			sort.Sort(buf)
			for _, res := range buf {
				select {
				case <-stop:
					return
				case results <- res:
				}
			}
			select {
			case <-stop:
				return
			case drained <- true:
			}
		}
	}
}

func (db *DB) followWAL(stream string, offset wal.Offset, partitions map[string]*partitionSpec, requests chan *partitionRequest) (func(), error) {
	var w *wal.WAL
	db.tablesMutex.RLock()
	w = db.streams[stream]
	db.tablesMutex.RUnlock()
	if w == nil {
		return nil, errors.New("Stream '%v' not found", stream)
	}

	db.log.Debugf("Following %v starting at %v", stream, offset)
	r, err := w.NewReader(fmt.Sprintf("clusterfollower.%v", stream), offset, db.walBuffers.Get)
	if err != nil {
		return nil, errors.New("Unable to open wal reader for %v", stream)
	}

	stop := make(chan bool, 1)
	finished := make(chan bool)
	db.Go(func(stopDB <-chan interface{}) {
		defer func() {
			r.Close()
			finished <- true
		}()

		for {
			data, err := r.Read()
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return
				}
				db.log.Debugf("Unable to read from stream '%v', continuing: %v", stream, err)
				continue
			}
			select {
			case <-stop:
				return
			case <-stopDB:
				return
			default:
				// keep going
			}
			if data == nil {
				// Ignore empty data
				continue
			}
			offset := r.Offset()
			metrics.CurrentlyReadingWAL(offset)
			select {
			case requests <- &partitionRequest{partitions, &walEntry{stream: stream, data: data, offset: offset}}:
				// okay
			case <-stop:
				return
			case <-stopDB:
				return
			}
		}
	})

	return func() {
		db.log.Debug("Calling wal.Stop()")
		r.Stop()
		close(stop)
		db.log.Debug("Waiting for WAL reading to finish")
		<-finished
	}, nil
}

type tableWithOffsets struct {
	t  *table
	os common.OffsetsBySource
}

func (to *tableWithOffsets) String() string {
	return fmt.Sprintf("%v - %v", to.t.Name, to.os)
}

func (db *DB) followLeaders(stream string, newSubscriber chan *tableWithOffsets, stop <-chan interface{}) {
	// Wait a little while for database to initialize
	// TODO: make this more rigorous, perhaps using eventual or something
	timer := time.NewTimer(30 * time.Second)
	var tables []*table
	var offsets []common.OffsetsBySource
	partitions := make(map[string]*common.Partition)

waitForTables:
	for {
		select {
		case <-stop:
			return
		case <-timer.C:
			if len(tables) == 0 {
				// Wait some more
				timer.Reset(5 * time.Second)
			}
			break waitForTables
		case subscriber := <-newSubscriber:
			db.log.Debugf("Got subscriber: %v", subscriber)
			table := subscriber.t
			os := subscriber.os
			tables = append(tables, table)
			offsets = append(offsets, os)
			partitionKeysString, partitionKeys := sortedPartitionKeys(table.PartitionBy)
			partition := partitions[partitionKeysString]
			if partition == nil {
				partition = &common.Partition{
					Keys: partitionKeys,
				}
				partitions[partitionKeysString] = partition
			}
			partition.Tables = append(partition.Tables, &common.PartitionTable{
				Name:    table.Name,
				Offsets: os,
			})
			// Got some tables, don't wait as long this time
			timer.Reset(1 * time.Second)
		}
	}

	for {
		cancel := make(chan bool, 100)
		db.Go(func(stop <-chan interface{}) {
			db.doFollowLeaders(stream, tables, offsets, partitions, cancel, stop)
		})
		select {
		case <-stop:
			return
		case subscriber := <-newSubscriber:
			select {
			case <-stop:
				return
			case cancel <- true:
				tables = append(tables, subscriber.t)
				offsets = append(offsets, subscriber.os)
			}
		}
	}
}

func (db *DB) doFollowLeaders(stream string, tables []*table, offsets []common.OffsetsBySource, partitions map[string]*common.Partition, cancel chan bool, stop <-chan interface{}) {
	var offsetsMx sync.RWMutex
	ins := make([]chan *walRead, 0, len(tables))
	for _, t := range tables {
		in := make(chan *walRead) // blocking channel so that we don't bother reading if we're in the middle of flushing
		ins = append(ins, in)
		db.Go(func(stop <-chan interface{}) {
			t.processInserts(in, stop)
		})
	}

	makeFollows := func(sources []int) map[int]*common.Follow {
		offsetsMx.RLock()
		earliestOffsetsBySource := make(map[int]wal.Offset)
		for _, source := range sources {
			earliestOffsetsBySource[source] = nil
		}
		for _, os := range offsets {
			for source, offset := range os {
				earliestOffset := earliestOffsetsBySource[source]
				if earliestOffset == nil || earliestOffset.After(offset) {
					earliestOffsetsBySource[source] = offset
				}
			}
		}
		offsetsMx.RUnlock()

		if db.opts.MaxFollowAge > 0 {
			earliestAllowedOffset := wal.NewOffsetForTS(db.clock.Now().Add(-1 * db.opts.MaxFollowAge))
			for source, earliestOffset := range earliestOffsetsBySource {
				if earliestAllowedOffset.After(earliestOffset) {
					db.log.Debugf("Forcibly limiting following from %d to %v", source, earliestAllowedOffset)
					earliestOffsetsBySource[source] = earliestAllowedOffset
				}
			}
		}

		db.log.Debugf("Following %v starting at %v", stream, earliestOffsetsBySource)
		follows := make(map[int]*common.Follow, len(earliestOffsetsBySource))
		for source, earliestOffset := range earliestOffsetsBySource {
			follows[source] = &common.Follow{
				Stream:         stream,
				EarliestOffset: earliestOffset,
				Partitions:     partitions,
				FollowerID:     common.FollowerID{db.opts.Partition, db.opts.ID},
			}
		}
		return follows
	}

	db.opts.Follow(makeFollows, func(data []byte, newOffset wal.Offset, source int) error {
		select {
		case <-cancel:
			// Canceled
			return errCanceled
		case <-stop:
			return errStopped
		default:
			// Okay to continue
		}

		for i, in := range ins {
			offsetsMx.Lock()
			priorOffsets := offsets[i]
			if priorOffsets == nil {
				priorOffsets = make(common.OffsetsBySource)
				offsets[i] = priorOffsets
			}
			priorOffset := priorOffsets[source]
			if newOffset.After(priorOffset) {
				in <- &walRead{data, newOffset, source}
				offsetsBySource := offsets[i]
				offsetsBySource[source] = newOffset
			}
			offsetsMx.Unlock()
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
