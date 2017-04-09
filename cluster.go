package zenodb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/mtime"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/planner"
	"github.com/spaolacci/murmur3"
	"hash"
	"runtime"
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
	if f.failed() {
		return
	}
	f.entries <- entry
}

func (f *follower) markFailed() {
	atomic.StoreInt32(&f.hasFailed, 1)
	close(f.entries)
}

func (f *follower) failed() bool {
	return atomic.LoadInt32(&f.hasFailed) == 1
}

func (db *DB) Follow(f *common.Follow, cb func([]byte, wal.Offset) error) {
	go db.processFollowersOnce.Do(db.processFollowers)
	fol := &follower{Follow: *f, cb: cb, entries: make(chan *walEntry, 1000000)} // TODO: make this buffer tunable
	db.followerJoined <- fol
	fol.read()
}

type tableSpec struct {
	where       goexpr.Expr
	whereString string
	followers   map[int][]*followSpec
}

type partitionSpec struct {
	keys   []string
	tables map[string]*tableSpec
}

func (db *DB) processFollowers() {
	log.Debug("Starting to process followers")

	nextFollowerID := 0
	followers := make(map[int]*follower)
	streams := make(map[string]map[string]*partitionSpec)
	stopWALReaders := make(map[string]func())
	includedFollowers := make([]int, 0, len(followers))

	stats := make([]int, db.opts.NumPartitions)
	statsInterval := 1 * time.Minute
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
				ps = &partitionSpec{keys: sortedKeys, tables: make(map[string]*tableSpec)}
				partitions[keys] = ps
			}
			for _, t := range partition.Tables {
				table := ps.tables[t.Name]
				if table == nil {
					tb := db.getTable(t.Name)
					if tb == nil {
						log.Errorf("Table %v requested by %d not found, not including from WAL", t.Name, f.PartitionNumber)
						continue
					}
					where := tb.Where
					whereString := ""
					if where != nil {
						whereString = strings.ToLower(where.String())
					}
					table = &tableSpec{
						where:       where,
						whereString: whereString,
						followers:   make(map[int][]*followSpec),
					}
					ps.tables[t.Name] = table
				}
				specs := table.followers[f.PartitionNumber]
				offset := t.Offset
				if f.EarliestOffset.After(offset) {
					offset = f.EarliestOffset
				}
				specs = append(specs, &followSpec{followerID: nextFollowerID, offset: offset})
				table.followers[f.PartitionNumber] = specs
			}
		}

		newlyJoinedStreams[f.Stream] = true
	}

	var requests chan *partitionRequest
	var results chan *partitionsResult

	for {
		select {
		case f := <-db.followerJoined:
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
							where:       table.where,
							whereString: table.whereString,
							followers:   make(map[int][]*followSpec, len(table.followers)),
						}
						partitionCopy.tables[tableName] = tableCopy
						for key, specs := range table.followers {
							tableCopy.followers[key] = specs
						}
					}
				}
			}
			streams = streamsCopy

			oldRequests := requests
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
				for _, partition := range streams[stream] {
					for _, table := range partition.tables {
						for _, specs := range table.followers {
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
				stopWALReader, err := db.followWAL(stream, earliestOffset, streams[stream], requests)
				if err != nil {
					log.Errorf("Unable to start following wal: %v", err)
					continue
				}
				stopWALReaders[stream] = stopWALReader

				if oldRequests != nil {
					close(oldRequests)
				}
			}

		case result := <-results:
			entry := result.entry
			partitions := streams[entry.stream]
			offset := entry.offset

			includedFollowers = includedFollowers[:0]
			for partitionKeys, partition := range partitions {
				pr := result.partitions[partitionKeys]
				pid := pr.pid
				for tableName, table := range partition.tables {
					specs := table.followers[pid]
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

			for _, f := range followers {
				log.Debugf("Queued for follower %d: %v", f.PartitionNumber, humanize.Comma(int64(len(f.entries))))
			}
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
	log.Debugf("Using %d CPUs to process entries for followers", parallelism)

	requests := make(chan *partitionRequest, parallelism*db.opts.NumPartitions*10) // TODO: make this tunable
	in := make(chan *partitionRequest, parallelism*db.opts.NumPartitions*10)
	mapped := make(chan *partitionsResult, parallelism*db.opts.NumPartitions*10)
	results := make(chan *partitionsResult, parallelism*db.opts.NumPartitions*10)
	queued := make(chan int)
	drained := make(chan bool)

	go db.enqueuePartitionRequests(parallelism, requests, in, queued, drained)
	for i := 0; i < parallelism; i++ {
		go db.mapPartitionRequests(in, mapped)
	}
	go db.reducePartitionRequests(parallelism, mapped, results, queued, drained)

	return requests, results
}

func (db *DB) enqueuePartitionRequests(parallelism int, requests chan *partitionRequest, in chan *partitionRequest, queued chan int, drained chan bool) {
	q := 0
	markQueued := func() {
		if q > 0 {
			queued <- q
			<-drained
			q = 0
		}
	}

requestsLoop:
	for {
		select {
		case req, more := <-requests:
			if req != nil {
				in <- req
				q++
				if q == parallelism {
					markQueued()
				}
			}
			if !more {
				break requestsLoop
			}
		default:
			markQueued()
			time.Sleep(1 * time.Second)
		}
	}
	markQueued()
	close(in)
	close(queued)
}

func (db *DB) mapPartitionRequests(in chan *partitionRequest, mapped chan *partitionsResult) {
	h := partitionHash()
	for req := range in {
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
				specs := table.followers[pid]
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
}

func (db *DB) reducePartitionRequests(parallelism int, mapped chan *partitionsResult, results chan *partitionsResult, queued chan int, drained chan bool) {
	buf := make(partitionsResultsByOffset, 0, parallelism)
	for numQueued := range queued {
		buf = buf[:0]
		for q := 0; q < numQueued; q++ {
			buf = append(buf, <-mapped)
		}
		sort.Sort(buf)
		for _, res := range buf {
			results <- res
		}
		drained <- true
	}
	close(results)
}

func (db *DB) followWAL(stream string, offset wal.Offset, partitions map[string]*partitionSpec, requests chan *partitionRequest) (func(), error) {
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
			case requests <- &partitionRequest{partitions, &walEntry{stream: stream, data: dataCopy, offset: r.Offset()}}:
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
	var offsetMx sync.RWMutex
	ins := make([]chan *walRead, 0, len(tables))
	for _, t := range tables {
		in := make(chan *walRead) // blocking channel so that we don't bother reading if we're in the middle of flushing
		ins = append(ins, in)
		go t.processInserts(in)
	}

	makeFollow := func() *common.Follow {
		offsetMx.RLock()
		var earliestOffset wal.Offset
		for i, offset := range offsets {
			if i == 0 || earliestOffset.After(offset) {
				earliestOffset = offset
			}
		}
		offsetMx.RUnlock()

		if db.opts.MaxFollowAge > 0 {
			earliestAllowedOffset := wal.NewOffsetForTS(db.clock.Now().Add(-1 * db.opts.MaxFollowAge))
			if earliestAllowedOffset.After(earliestOffset) {
				log.Debugf("Forcibly limiting following to %v", earliestAllowedOffset)
				earliestOffset = earliestAllowedOffset
			}
		}

		log.Debugf("Following %v starting at %v", stream, earliestOffset)
		return &common.Follow{
			Stream:          stream,
			EarliestOffset:  earliestOffset,
			PartitionNumber: db.opts.Partition,
			Partitions:      partitions,
		}
	}

	db.opts.Follow(makeFollow, func(data []byte, newOffset wal.Offset) error {
		select {
		case <-cancel:
			// Canceled
			return errCanceled
		default:
			// Okay to continue
		}

		for i, in := range ins {
			priorOffset := offsets[i]
			if newOffset.After(priorOffset) {
				in <- &walRead{data, newOffset}
				offsetMx.Lock()
				offsets[i] = newOffset
				offsetMx.Unlock()
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

func (db *DB) queryForRemote(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onFields core.OnFields, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
	source, err := db.Query(sqlString, isSubQuery, subQueryResults, shouldIncludeMemStore(ctx))
	if err != nil {
		return err
	}
	elapsed := mtime.Stopwatch()
	defer func() {
		log.Debugf("Processed query in %v: %v", elapsed(), sqlString)
	}()
	if unflat {
		return core.UnflattenOptimized(source).Iterate(ctx, onFields, onRow)
	}
	return source.Iterate(ctx, onFields, onFlatRow)
}

type resultInfo struct {
	partition int
	key       bytemap.ByteMap
	vals      core.Vals
	flatRow   *core.FlatRow
	err       error
}

func (db *DB) queryCluster(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, includeMemStore bool, unflat bool, onFields core.OnFields, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
	ctx = withIncludeMemStore(ctx, includeMemStore)
	numPartitions := db.opts.NumPartitions
	results := make(chan *remoteResult, numPartitions*100000) // TODO: make this tunable
	resultsByPartition := make(map[int]*int64)
	var _finalErr error
	var finalErrMx sync.RWMutex
	finalErr := func() error {
		finalErrMx.RLock()
		result := _finalErr
		finalErrMx.RUnlock()
		return result
	}
	fail := func(err error) {
		finalErrMx.Lock()
		if _finalErr != nil {
			_finalErr = err
		}
		finalErrMx.Unlock()
	}
	_stopped := int64(0)
	stopped := func() bool {
		return atomic.LoadInt64(&_stopped) == 1
	}
	stop := func() {
		atomic.StoreInt64(&_stopped, 1)
	}

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
					results <- &remoteResult{
						partition: partition,
						totalRows: 0,
						err:       nil,
					}
					break
				}

				var partOnRow func(key bytemap.ByteMap, vals core.Vals) (bool, error)
				var partOnFlatRow func(row *core.FlatRow) (bool, error)
				if unflat {
					partOnRow = func(key bytemap.ByteMap, vals core.Vals) (bool, error) {
						err := finalErr()
						if err != nil {
							return false, err
						}
						if stopped() {
							return false, nil
						}
						results <- &remoteResult{
							partition: partition,
							key:       key,
							vals:      vals,
						}
						atomic.AddInt64(resultsForPartition, 1)
						return true, nil
					}
				} else {
					partOnFlatRow = func(row *core.FlatRow) (bool, error) {
						err := finalErr()
						if err != nil {
							return false, err
						}
						if stopped() {
							return false, nil
						}
						results <- &remoteResult{
							partition: partition,
							flatRow:   row,
						}
						atomic.AddInt64(resultsForPartition, 1)
						return true, nil
					}
				}

				err := query(subCtx, sqlString, isSubQuery, subQueryResults, unflat, func(fields core.Fields) error {
					results <- &remoteResult{
						partition: partition,
						fields:    fields,
					}
					return nil
				}, partOnRow, partOnFlatRow)
				if err != nil && atomic.LoadInt64(resultsForPartition) == 0 {
					log.Debugf("Failed on partition %d, haven't read anything, continuing: %v", partition, err)
					continue
				}
				results <- &remoteResult{
					partition: partition,
					totalRows: int(atomic.LoadInt64(resultsForPartition)),
					err:       err,
				}
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
	hasFields := false
	resultCount := 0
	for pendingPartitions := numPartitions; pendingPartitions > 0; {
		start := time.Now()
		select {
		case result := <-results:
			// first handle fields
			if result.fields != nil {
				if !hasFields {
					err := onFields(result.fields)
					if err != nil {
						fail(err)
					}
					hasFields = true
				}
				continue
			}

			// handle unflat rows
			if result.key != nil {
				if stopped() || finalErr() != nil {
					continue
				}
				more, err := onRow(result.key, result.vals)
				if err != nil {
					fail(err)
				} else if !more {
					stop()
				}
				continue
			}

			// handle flat rows
			if result.flatRow != nil {
				if stopped() || finalErr() != nil {
					continue
				}
				more, err := onFlatRow(result.flatRow)
				if err != nil {
					fail(err)
					return err
				} else if !more {
					stop()
				}
				continue
			}

			// final results for partition
			resultCount++
			pendingPartitions--
			if result.err != nil {
				log.Errorf("Error from partition %d: %v", result.partition, result.err)
				fail(result.err)
			}
			delta := time.Now().Sub(start)
			log.Debugf("%d/%d got %d results from partition %d in %v", resultCount, db.opts.NumPartitions, result.totalRows, result.partition, delta)
			delete(resultsByPartition, result.partition)
		case <-timeout.C:
			fail(core.ErrDeadlineExceeded)
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
			return finalErr()
		}
	}

	return finalErr()
}

type remoteResult struct {
	partition int
	fields    core.Fields
	key       bytemap.ByteMap
	vals      core.Vals
	flatRow   *core.FlatRow
	totalRows int
	err       error
}
