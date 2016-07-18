package tdb

import (
	"time"

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
	select {
	case t.inserts <- &insert{key, vals}:
		t.statsMutex.Lock()
		t.stats.QueuedPoints++
		t.statsMutex.Unlock()
	default:
		t.statsMutex.Lock()
		t.stats.DroppedPoints++
		t.statsMutex.Unlock()
	}
}

func (t *table) process() {
	// Commits are primarily driven by batch size, but the commitTicker makes sure
	// that we're commiting at least every 5 seconds.
	// TODO - make this tunable
	commitTicker := time.NewTicker(5 * time.Second)

	batch := gorocksdb.NewWriteBatch()
	commit := func() {
		count := int64(batch.Count() / len(t.Fields))
		err := t.rdb.Write(defaultWriteOptions, batch)
		batch = gorocksdb.NewWriteBatch()
		if err != nil {
			t.log.Errorf("Unable to commit batch: %v", err)
			t.statsMutex.Lock()
			t.stats.DroppedPoints += count
			t.statsMutex.Unlock()
		} else {
			t.statsMutex.Lock()
			t.stats.InsertedPoints += count
			t.statsMutex.Unlock()
		}
	}

	for {
		select {
		case i := <-t.inserts:
			for _, field := range t.Fields {
				batch.Merge(keyWithField(i.key, field.Name), i.vals)
			}
			t.statsMutex.Lock()
			t.stats.QueuedPoints--
			t.statsMutex.Unlock()
			if batch.Count() >= int(t.batchSize) {
				commit()
			}
		case <-commitTicker.C:
			commit()
		}
	}
}
