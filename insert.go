package tdb

import (
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
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
	vals tsparams
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
	vals := newTSParams(point.Ts, bytemap.NewFloat(point.Vals))
	t.inserts <- &insert{key, vals}
	t.statsMutex.Lock()
	t.stats.QueuedPoints++
	t.statsMutex.Unlock()
}

func (t *table) process() {
	commitTicker := t.clock.NewTicker(t.Resolution)
	batch := gorocksdb.NewWriteBatch()
	for {
		select {
		case i := <-t.inserts:
			for _, field := range t.Fields {
				batch.MergeCF(t.data, keyWithField(i.key, field.Name), i.vals)
			}
			t.statsMutex.Lock()
			t.stats.QueuedPoints--
			t.statsMutex.Unlock()
		case <-commitTicker.C:
			err := t.rdb.Write(defaultWriteOptions, batch)
			if err != nil {
				t.log.Errorf("Unable to commit batch: %v", err)
			}
			count := int64(batch.Count() / len(t.Fields))
			t.statsMutex.Lock()
			t.stats.InsertedPoints += count
			t.statsMutex.Unlock()
			batch = gorocksdb.NewWriteBatch()
		}
	}
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
	it := t.rdb.NewIteratorCF(ro, t.data)
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		v := it.Value()
		vd := v.Data()
		if vd == nil || len(vd) < width64bits || sequence(vd).start().Before(retainUntil) {
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
