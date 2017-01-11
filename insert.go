package zenodb

import (
	"fmt"
	"hash"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/encoding"
)

func (db *DB) Insert(stream string, ts time.Time, dims map[string]interface{}, vals map[string]float64) error {
	return db.InsertRaw(stream, ts, bytemap.New(dims), bytemap.NewFloat(vals))
}

func (db *DB) InsertRaw(stream string, ts time.Time, dims bytemap.ByteMap, vals bytemap.ByteMap) error {
	if db.opts.Follow != nil {
		return errors.New("Declining to insert data directly to follower")
	}

	stream = strings.TrimSpace(strings.ToLower(stream))
	db.tablesMutex.Lock()
	w := db.streams[stream]
	db.tablesMutex.Unlock()
	if w == nil {
		return fmt.Errorf("No wal found for stream %v", stream)
	}

	var lastErr error
	tsd := make([]byte, encoding.Width64bits)
	encoding.EncodeTime(tsd, ts)
	dimsLen := make([]byte, encoding.Width32bits)
	encoding.WriteInt32(dimsLen, len(dims))
	valsLen := make([]byte, encoding.Width32bits)
	encoding.WriteInt32(valsLen, len(vals))
	_, err := w.Write(tsd, dimsLen, dims, valsLen, vals)
	if err != nil {
		log.Error(err)
		if lastErr == nil {
			lastErr = err
		}
	}
	return lastErr
}

func (t *table) processInserts() {
	isFollower := t.db.opts.Follow != nil
	start := time.Now()
	inserted := 0
	skipped := 0
	bytesRead := 0

	h := partitionHash()
	for {
		data, err := t.wal.Read()
		if err != nil {
			panic(fmt.Errorf("Unable to read from WAL: %v", err))
		}
		if data == nil {
			// Ignore empty data
			continue
		}
		bytesRead += len(data)
		if t.insert(data, isFollower, h, t.wal.Offset()) {
			inserted++
		} else {
			// Did not insert (probably due to WHERE clause)
			skipped++
		}
		delta := time.Now().Sub(start)
		if delta > 1*time.Minute {
			t.log.Debugf("Read %v at %v per second", humanize.Bytes(uint64(bytesRead)), humanize.Bytes(uint64(float64(bytesRead)/delta.Seconds())))
			t.log.Debugf("Inserted %v points at %v per second", humanize.Comma(int64(inserted)), humanize.Commaf(float64(inserted)/delta.Seconds()))
			t.log.Debugf("Skipped %v points at %v per second", humanize.Comma(int64(skipped)), humanize.Commaf(float64(skipped)/delta.Seconds()))
			inserted = 0
			skipped = 0
			bytesRead = 0
			start = time.Now()
		}
	}
}

func (t *table) insert(data []byte, isFollower bool, h hash.Hash32, offset wal.Offset) bool {
	tsd, data := encoding.Read(data, encoding.Width64bits)
	ts := encoding.TimeFromBytes(tsd)
	if ts.Before(t.truncateBefore()) {
		// Ignore old data
		return false
	}
	dimsLen, remain := encoding.ReadInt32(data)
	dims, remain := encoding.Read(remain, dimsLen)
	if isFollower && !t.db.inPartition(h, dims, t.PartitionBy, t.db.opts.Partition) {
		log.Debug("Data not relevant to partition")
		// data not relevant to follower on this table
		return false
	}
	log.Debug("Data relevant to partition")

	valsLen, remain := encoding.ReadInt32(remain)
	vals, _ := encoding.Read(remain, valsLen)
	// Split the dims and vals so that holding on to one doesn't force holding on
	// to the other. Also, we need copies for both because the WAL read buffer
	// will change on next call to wal.Read().
	dimsBM := make(bytemap.ByteMap, len(dims))
	valsBM := make(bytemap.ByteMap, len(vals))
	copy(dimsBM, dims)
	copy(valsBM, vals)
	return t.doInsert(ts, dimsBM, valsBM, offset)
}

func (t *table) doInsert(ts time.Time, dims bytemap.ByteMap, vals bytemap.ByteMap, offset wal.Offset) bool {
	where := t.getWhere()

	if where != nil {
		ok := where.Eval(dims)
		if !ok.(bool) {
			if t.log.IsTraceEnabled() {
				t.log.Tracef("Filtering out inbound point at %v due to %v: %v", ts, where, dims.AsMap())
			}
			t.statsMutex.Lock()
			t.stats.FilteredPoints++
			t.statsMutex.Unlock()
			return false
		}
	}
	t.db.clock.Advance(ts)

	if t.log.IsTraceEnabled() {
		t.log.Tracef("Including inbound point at %v: %v", ts, dims.AsMap())
	}

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
	t.db.capMemStoreSize()
	t.rowStore.insert(&insert{key, tsparams, dims, offset})
	t.statsMutex.Lock()
	t.stats.InsertedPoints++
	t.statsMutex.Unlock()

	return true
}

func (t *table) recordQueued() {
	t.statsMutex.Lock()
	t.stats.QueuedPoints++
	t.statsMutex.Unlock()
}
