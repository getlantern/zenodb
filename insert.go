package zenodb

import (
	"fmt"
	"strings"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/encoding"
)

func (db *DB) Insert(stream string, ts time.Time, dims map[string]interface{}, vals map[string]float64) error {
	return db.InsertRaw(stream, ts, bytemap.New(dims), bytemap.NewFloat(vals))
}

func (db *DB) InsertRaw(stream string, ts time.Time, dims bytemap.ByteMap, vals bytemap.ByteMap) error {
	stream = strings.TrimSpace(strings.ToLower(stream))
	db.tablesMutex.Lock()
	w := db.streams[stream]
	db.tablesMutex.Unlock()
	if w == nil {
		return fmt.Errorf("No wal found for stream %v", stream)
	}

	tsd := make([]byte, encoding.Width64bits)
	encoding.EncodeTime(tsd, ts)
	dimsLen := make([]byte, encoding.Width32bits)
	encoding.WriteInt32(dimsLen, len(dims))
	valsLen := make([]byte, encoding.Width32bits)
	encoding.WriteInt32(valsLen, len(vals))
	_, err := w.Write(tsd, dimsLen, dims, valsLen, vals)
	return err
}

func (t *table) processInserts() {
	start := time.Now()
	inserted := 0
	for {
		data, err := t.wal.Read()
		if err != nil {
			panic(fmt.Errorf("Unable to read from WAL: %v", err))
		}
		tsd, data := encoding.Read(data, encoding.Width64bits)
		dimsLen, data := encoding.ReadInt32(data)
		dims, data := encoding.Read(data, dimsLen)
		valsLen, data := encoding.ReadInt32(data)
		vals, data := encoding.Read(data, valsLen)
		offset := t.wal.Offset()
		t.insert(encoding.TimeFromBytes(tsd), bytemap.ByteMap(dims), bytemap.ByteMap(vals), offset)
		inserted++
		delta := time.Now().Sub(start)
		if delta > 1*time.Minute {
			t.log.Debugf("Inserted %d points at %f per second", inserted, float64(inserted)/delta.Seconds())
			inserted = 0
			start = time.Now()
		}
	}
}

func (t *table) insert(ts time.Time, dims bytemap.ByteMap, vals bytemap.ByteMap, offset wal.Offset) {
	t.whereMutex.RLock()
	where := t.Where
	t.whereMutex.RUnlock()

	if where != nil {
		ok := where.Eval(dims)
		if !ok.(bool) {
			t.log.Tracef("Filtering out inbound point: %v", dims)
			t.statsMutex.Lock()
			t.stats.FilteredPoints++
			t.statsMutex.Unlock()
			return
		}
	}
	t.db.clock.Advance(ts)

	var key bytemap.ByteMap
	if len(t.GroupBy) == 0 {
		key = dims
	} else {
		// Reslice dimensions
		names := make([]string, 0, len(t.GroupBy))
		values := make([]interface{}, 0, len(t.GroupBy))
		for _, groupBy := range t.GroupBy {
			val := groupBy.Expr.Eval(dims)
			if val != nil {
				names = append(names, groupBy.Name)
				values = append(values, val)
			}
		}
		key = bytemap.FromSortedKeysAndValues(names, values)
	}

	tsparams := encoding.NewTSParams(ts, vals)
	t.rowStore.insert(&insert{key, tsparams, dims, offset})
	t.statsMutex.Lock()
	t.stats.InsertedPoints++
	t.statsMutex.Unlock()
}

func (t *table) recordQueued() {
	t.statsMutex.Lock()
	t.stats.QueuedPoints++
	t.statsMutex.Unlock()
}
