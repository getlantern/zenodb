package zenodb

import (
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/encoding"
	"hash/crc32"
	"math/rand"
)

type Follow struct {
	Stream    string
	Offset    wal.Offset
	Partition int
}

type RegisterQueryHandler struct {
	Partition int
}

type QueryRemote func(sql string, onValue func(bytemap.ByteMap, []encoding.Sequence)) error

func (db *DB) Follow(f *Follow, cb func([]byte, wal.Offset) error) error {
	db.tablesMutex.RLock()
	w := db.streams[f.Stream]
	db.tablesMutex.RUnlock()
	if w == nil {
		return errors.New("Stream '%v' not found", f.Stream)
	}

	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	r, err := w.NewReader("follower."+f.Stream, f.Offset)
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
			log.Trace("Sending to follower")
			err = cb(data, r.Offset())
			if err != nil {
				log.Debug(err)
				return err
			}
		}
	}
}

func (db *DB) RegisterQueryHandler(r *RegisterQueryHandler, query QueryRemote) {
	db.tablesMutex.Lock()
	defer db.tablesMutex.Unlock()
	db.remoteQueryHandlers[r.Partition] = append(db.remoteQueryHandlers[r.Partition], query)
}

type remoteQueryable struct {
	*table
	db  *DB
	sql string
}

func (rq *remoteQueryable) iterate(fields []string, onValue func(bytemap.ByteMap, []encoding.Sequence)) error {
	rq.db.tablesMutex.RLock()
	handlers := make(map[int][]QueryRemote, len(rq.db.remoteQueryHandlers))
	for k, v := range rq.db.remoteQueryHandlers {
		handlers[k] = v
	}
	rq.db.tablesMutex.RUnlock()

	results := make(chan error, rq.db.opts.NumPartitions)
	expectedResults := 0
	for i := 0; i < rq.db.opts.NumPartitions; i++ {
		qhs := handlers[i]
		if len(qhs) == 0 {
			log.Debugf("No live handlers for partition %d, skipping!", i)
			continue
		}
		expectedResults++
		qh := qhs[rand.Intn(len(qhs))]
		go func() {
			results <- qh(rq.sql, onValue)
		}()
	}

	for i := 0; i < expectedResults; i++ {
		err := <-results
		if err != nil {
			return err
		}
	}

	return nil
}
