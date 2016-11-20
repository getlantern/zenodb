package zenodb

import (
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/sql"
	"github.com/spaolacci/murmur3"
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
		// Skip timestamp
		_, remain := encoding.Read(data, encoding.Width64bits)
		dimsLen, remain := encoding.ReadInt32(remain)
		dims, remain := encoding.Read(remain, dimsLen)
		h.Reset()
		h.Write(dims)
		if int(h.Sum32())%db.opts.NumPartitions == f.Partition {
			err = cb(data, r.Offset())
			if err != nil {
				log.Debug(err)
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

func (rq *remoteQueryable) iterate(fields []string, includeMemStore bool, onValue func(bytemap.ByteMap, []encoding.Sequence)) error {
	numPartitions := rq.db.opts.NumPartitions
	results := make(chan error, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partition := i
		go func() {
			for {
				query := rq.db.remoteQueryHandlerForPartition(partition)
				if query == nil {
					log.Errorf("No query handler for partition %d, ignoring", partition)
					results <- nil
					break
				}

				hasReadResult, err := query(rq.query.SQL, rq.exec.includeMemStore, rq.exec.isSubQuery, rq.exec.subQueryResults, func(key bytemap.ByteMap, values []encoding.Sequence) {
					onValue(key, values)
				})
				if err != nil && !hasReadResult {
					continue
				}
				results <- err
				break
			}
		}()
	}

	var finalErr error
	for i := 0; i < numPartitions; i++ {
		err := <-results
		if err != nil && finalErr == nil {
			finalErr = err
		}
	}

	return finalErr
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
