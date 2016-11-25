package zenodb

import (
	"bytes"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/sql"
	"github.com/spaolacci/murmur3"
	"sync"
	"sync/atomic"
	"time"
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
		dims, remain := encoding.Read(remain, dimsLen)
		h.Reset()
		h.Write(dims)
		if int(h.Sum32())%db.opts.NumPartitions == f.Partition {
			err = cb(data, r.Offset())
			if err != nil {
				log.Debugf("Unable to write to follower: %v", err)
				return err
			}
		}
	}
}

func (db *DB) RegisterQueryHandler(partition int, query QueryRemote) {
	db.tablesMutex.Lock()
	handlersCh := db.remoteQueryHandlers[partition]
	if handlersCh == nil {
		// TODO: maybe make size based on configuration or something
		handlersCh = make(chan QueryRemote, 100)
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
				go handler("", false, false, nil, nil)
			}
		}
	}
}

func (db *DB) remoteQueryHandlerForPartition(partition int) QueryRemote {
	db.tablesMutex.RLock()
	defer db.tablesMutex.RUnlock()
	select {
	case handler := <-db.remoteQueryHandlers[partition]:
		return handler
	default:
		return nil
	}
}

type remoteQueryable struct {
	*table
	exec  *queryExecution
	query *sql.Query
	res   time.Duration
}

func (rq *remoteQueryable) fields() []sql.Field {
	return rq.query.Fields
}

func (rq *remoteQueryable) resolution() time.Duration {
	return rq.res
}

func (rq *remoteQueryable) retentionPeriod() time.Duration {
	return rq.table.retentionPeriod()
}

func (rq *remoteQueryable) truncateBefore() time.Time {
	return rq.table.truncateBefore()
}

type remoteResult struct {
	partition    int
	handlerFound bool
	results      int
	err          error
}

func (rq *remoteQueryable) iterate(fields []string, includeMemStore bool, onValue func(bytemap.ByteMap, []encoding.Sequence)) error {
	numPartitions := rq.db.opts.NumPartitions
	results := make(chan *remoteResult, numPartitions)
	resultsByPartition := make(map[int]*int64)
	timedOut := false
	var mx sync.RWMutex

	for i := 0; i < numPartitions; i++ {
		partition := i
		_resultsForPartition := int64(0)
		resultsForPartition := &_resultsForPartition
		resultsByPartition[partition] = resultsForPartition
		go func() {
			for {
				query := rq.db.remoteQueryHandlerForPartition(partition)
				if query == nil {
					log.Errorf("No query handler for partition %d, ignoring", partition)
					results <- &remoteResult{partition, false, 0, nil}
					break
				}

				hasReadResult, err := query(rq.query.SQL, rq.exec.includeMemStore, rq.exec.isSubQuery, rq.exec.subQueryResults, func(key bytemap.ByteMap, values []encoding.Sequence) {
					mx.RLock()
					if !timedOut {
						atomic.AddInt64(resultsForPartition, 1)
						onValue(key, values)
					}
					mx.RUnlock()
				})
				if err != nil && !hasReadResult {
					log.Debugf("Failed on partition %d, haven't read anything, continuing: %v", partition, err)
					continue
				}
				results <- &remoteResult{partition, true, int(atomic.LoadInt64(resultsForPartition)), err}
				break
			}
		}()
	}

	// TODO: get this from context
	minTimeout := 5 * time.Second
	nextTimeout := 24 * time.Hour
	timeout := time.NewTimer(nextTimeout)
	maxDelta := 0 * time.Second
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
			if result.handlerFound {
				delta := time.Now().Sub(start)
				if delta > maxDelta {
					maxDelta = delta
				}
				// Reduce timer based on how quickly existing results returned
				nextTimeout = maxDelta * 2
				if nextTimeout < minTimeout {
					nextTimeout = minTimeout
				}
				timeout.Reset(nextTimeout)
			}
			log.Debugf("%d/%d got %d results from partition %d, next timeout: %v", resultCount, rq.db.opts.NumPartitions, result.results, result.partition, nextTimeout)
			delete(resultsByPartition, result.partition)
		case <-timeout.C:
			mx.Lock()
			timedOut = true
			mx.Unlock()
			log.Errorf("Failed to get results within timeout, %d of %d partitions reporting", resultCount, numPartitions)
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
