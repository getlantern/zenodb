package tdb

import (
	"time"

	"github.com/getlantern/bytemap"
	"github.com/tecbot/gorocksdb"
)

var (
	defaultWriteOptions = gorocksdb.NewDefaultWriteOptions()
	defaultReadOptions  = gorocksdb.NewDefaultReadOptions()
)

func init() {
	// Don't bother filling the cache, since we're always doing full scans anyway
	defaultReadOptions.SetFillCache(false)
	// Make it a tailing iterator so that it doesn't create a snapshot (which
	// would have the effect of reducing the efficiency of applying merge ops
	// during compaction)
	defaultReadOptions.SetTailing(true)
}

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
				batch.Merge(keyWithField(i.key, field.Name), i.vals)
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

func (t *table) archivePeriod() time.Duration {
	return t.hotPeriod / 10
}
