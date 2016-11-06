package zenodb

import (
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
)

type Follow struct {
	Stream string
	Offset wal.Offset
}

func (db *DB) Follow(f *Follow, cb func([]byte, wal.Offset) error) error {
	db.tablesMutex.RLock()
	w := db.streams[f.Stream]
	db.tablesMutex.RUnlock()
	if w == nil {
		return errors.New("Stream '%v' not found", f.Stream)
	}
	r, err := w.NewReader("follower."+f.Stream, f.Offset)
	if err != nil {
		return errors.New("Unable to open wal reader for %v", f.Stream)
	}
	for {
		b, err := r.Read()
		if err != nil {
			log.Debugf("Unable to read from stream '%v': %v", f.Stream, err)
			continue
		}
		log.Debug("Sending to follower")
		err = cb(b, r.Offset())
		if err != nil {
			log.Debug(err)
			return err
		}
	}
}
