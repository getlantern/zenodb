package tdb

import (
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
	"github.com/tecbot/gorocksdb"
)

var (
	defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()
)

type Point struct {
	Ts   time.Time
	Dims map[string]interface{}
	Vals map[string]float64
}

// Get implements the interface govaluate.Parameters
func (p *Point) Get(name string) (interface{}, error) {
	result := p.Dims[name]
	if result == nil {
		result = ""
	}
	return result, nil
}

type insert struct {
	key  bytemap.ByteMap
	vals []byte
}

func (db *DB) Insert(stream string, point *Point) error {
	db.tablesMutex.Lock()
	s := db.streams[stream]
	db.tablesMutex.Unlock()

	for _, t := range s {
		t.insert(point)
	}
	return nil
}

func (t *table) insert(point *Point) {
	t.whereMutex.RLock()
	where := t.where
	t.whereMutex.RUnlock()

	if where != nil {
		ok, err := where.Eval(point)
		if err != nil {
			t.log.Errorf("Unable to filter inbound point: %v", err)
			t.statsMutex.Lock()
			t.stats.DroppedPoints++
			t.statsMutex.Unlock()
		} else if !ok.(bool) {
			t.log.Tracef("Filtering out inbound point: %v", point.Dims)
			t.statsMutex.Lock()
			t.stats.FilteredPoints++
			t.statsMutex.Unlock()
			return
		}
	}
	t.clock.Advance(point.Ts)

	if len(t.GroupBy) > 0 {
		// Reslice dimensions
		newDims := make(map[string]interface{}, len(t.GroupBy))
		for _, dim := range t.GroupBy {
			newDims[dim] = point.Dims[dim]
		}
		point = &Point{
			Ts:   point.Ts,
			Dims: newDims,
			Vals: point.Vals,
		}
	}

	key := bytemap.New(point.Dims)
	vals, err := paramsAsBytes(point.Vals)
	if err != nil {
		t.log.Errorf("Unable to serialize value bytes: %v", err)
		return
	}
	t.inserts <- &insert{keyWithTime(key, point.Ts), vals}
	t.statsMutex.Lock()
	t.stats.QueuedPoints++
	t.statsMutex.Unlock()
}

func (t *table) process() {
	commitTicker := t.clock.NewTicker(t.Resolution)
	archiveTicker := t.clock.NewTicker(t.Resolution * 2)
	batch := gorocksdb.NewWriteBatch()
	for {
		select {
		case i := <-t.inserts:
			batch.PutCF(t.dirty, i.key, nil)
			batch.MergeCF(t.accum, i.key, i.vals)
			t.statsMutex.Lock()
			t.stats.QueuedPoints--
			t.statsMutex.Unlock()
		case <-commitTicker.C:
			err := t.rdb.Write(defaultWriteOptions, batch)
			if err != nil {
				t.log.Errorf("Unable to commit batch: %v", err)
			}
			count := int64(batch.Count() / 2)
			t.statsMutex.Lock()
			t.stats.InsertedPoints += count
			t.statsMutex.Unlock()
			batch = gorocksdb.NewWriteBatch()
		case <-archiveTicker.C:
			t.archiveRequests <- t.rdb.NewSnapshot()
		}
	}
}

func (t *table) processArchiving() {
	for snap := range t.archiveRequests {
		t.archiveDirty(snap)
	}
}

func (t *table) archiveDirty(snap *gorocksdb.Snapshot) {
	t.log.Debug("Archiving dirty")
	start := time.Now()
	i := 0
	defer func() {
		t.log.Debugf("Done archiving %d dirty in %v", i, time.Now().Sub(start))
	}()

	var keysToFree []*gorocksdb.Slice
	defer func() {
		for _, key := range keysToFree {
			key.Free()
		}
	}()

	batch := gorocksdb.NewWriteBatch()
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetSnapshot(snap)
	ro.SetFillCache(true)
	acit := t.rdb.NewIteratorCF(ro, t.accum)
	acit.SeekToFirst()
	dit := t.rdb.NewIteratorCF(ro, t.dirty)
	for dit.SeekToFirst(); dit.Valid(); dit.Next() {
		i++
		_k := dit.Key()
		keysToFree = append(keysToFree, _k)
		k := _k.Data()
		batch.DeleteCF(t.dirty, k)
		acit.Seek(k)
		if !acit.Valid() {
			t.log.Error("Skipping missing accumulator for dirty key, this should not happen!")
			continue
		}
		_val := acit.Value()
		val := _val.Data()
		ts, key := timeAndKey(k)
		acs := t.accumulators(val)
		for i, ac := range acs {
			field := t.Fields[i].Name
			batch.MergeCF(t.archive, keyWithField(key, field), newTSValue(ts, ac.Get()))
		}
		_val.Free()
	}

	count := int64(batch.Count() / 2 / len(t.Fields))
	err := t.rdb.Write(defaultWriteOptions, batch)
	if err != nil {
		t.log.Errorf("Write failed, discarding batch: %v", err)
		t.statsMutex.Lock()
		t.stats.DroppedPoints += count
		t.statsMutex.Unlock()
	} else {
		t.statsMutex.Lock()
		t.stats.ArchivedPoints += count
		t.statsMutex.Unlock()
	}

	snap.Release()
}

func (t *table) retain() {
	retentionTicker := t.clock.NewTicker(t.retentionPeriod)
	wo := gorocksdb.NewDefaultWriteOptions()
	for range retentionTicker.C {
		t.doRetain(wo)
	}
}

func (t *table) doRetain(wo *gorocksdb.WriteOptions) {
	t.log.Trace("Removing expired keys")
	start := time.Now()
	batch := gorocksdb.NewWriteBatch()
	var keysToFree []*gorocksdb.Slice
	defer func() {
		for _, k := range keysToFree {
			k.Free()
		}
	}()

	retainUntil := t.clock.Now().Add(-1 * t.retentionPeriod)
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it := t.rdb.NewIteratorCF(ro, t.archive)
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		v := it.Value()
		vd := v.Data()
		if vd == nil || len(vd) < size64bits || sequence(vd).start().Before(retainUntil) {
			k := it.Key()
			keysToFree = append(keysToFree, k)
			batch.Delete(k.Data())
		}
		v.Free()
	}

	if batch.Count() > 0 {
		err := t.rdb.Write(wo, batch)
		if err != nil {
			t.log.Errorf("Unable to remove expired keys: %v", err)
		} else {
			delta := time.Now().Sub(start)
			expiredPoints := int64(batch.Count() / len(t.Fields))
			t.statsMutex.Lock()
			t.stats.ExpiredPoints += expiredPoints
			t.statsMutex.Unlock()
			t.log.Tracef("Removed %v expired points in %v", humanize.Comma(expiredPoints), delta)
		}
	} else {
		t.log.Trace("No expired keys to remove")
	}
}

func (t *table) archivePeriod() time.Duration {
	return t.hotPeriod / 10
}

func floatsToValues(in map[string]float64) map[string]expr.Value {
	out := make(map[string]expr.Value, len(in))
	for key, value := range in {
		out[key] = expr.Float(value)
	}
	return out
}
