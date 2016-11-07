package zenodb

import (
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/encoding"
	"hash/crc32"
)

type Follow struct {
	Stream    string
	Offset    wal.Offset
	Partition int
}

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
