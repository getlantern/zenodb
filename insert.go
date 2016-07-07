package tdb

import (
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
	"github.com/spaolacci/murmur3"
	"github.com/tecbot/gorocksdb"
)

type Point struct {
	Ts   time.Time
	Dims map[string]interface{}
	Vals map[string]float64
}

type partition struct {
	t            *table
	archiveDelay time.Duration
	inserts      chan *insert
	tail         map[string]*bucket
}

type insert struct {
	ts     time.Time
	t      *table
	key    bytemap.ByteMap
	vals   map[string]expr.Value
	bucket *bucket
}

type archiveRequest struct {
	key string
	b   *bucket
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
	t.clock.Advance(point.Ts)

	if len(t.groupBy) > 0 {
		// Reslice dimensions
		newDims := make(map[string]interface{}, len(t.groupBy))
		for _, dim := range t.groupBy {
			newDims[dim] = point.Dims[dim]
		}
		point = &Point{
			Ts:   point.Ts,
			Dims: newDims,
			Vals: point.Vals,
		}
	}

	vals := floatsToValues(point.Vals)
	key := bytemap.New(point.Dims)
	h := int(murmur3.Sum32(key))
	p := h % len(t.partitions)
	select {
	case t.partitions[p].inserts <- &insert{point.Ts, t, key, vals, nil}:
		t.statsMutex.Lock()
		t.stats.InsertedPoints++
		t.statsMutex.Unlock()
	default:
		t.statsMutex.Lock()
		t.stats.DroppedPoints++
		t.statsMutex.Unlock()
	}
}

func (p *partition) processInserts() {
	archivePeriod := p.t.archivePeriod()
	p.t.log.Debugf("Archiving every %v, delayed by %v", archivePeriod, p.archiveDelay)
	archiveTicker := p.t.clock.NewTicker(archivePeriod)
	for {
		select {
		case insert := <-p.inserts:
			p.insert(insert)
		case <-archiveTicker.C:
			p.requestArchiving()
		}
	}
}

func (p *partition) insert(insert *insert) {
	now := p.t.clock.Now()
	start := roundTime(insert.ts, p.t.resolution)
	if now.Sub(start) > p.t.hotPeriod {
		p.t.statsMutex.Lock()
		p.t.stats.InsertedPoints--
		p.t.stats.DroppedPoints++
		p.t.statsMutex.Unlock()
		if p.t.log.IsTraceEnabled() {
			p.t.log.Tracef("Discarding insert at %v outside of hot period starting at %v", start.In(time.UTC), now.Add(-1*p.t.hotPeriod))
		}
		return
	}
	b := p.tail[string(insert.key)]
	if b == nil || b.start.Before(start) {
		p.t.statsMutex.Lock()
		if b == nil {
			p.t.stats.HotKeys++
		}
		p.t.statsMutex.Unlock()
		b = &bucket{start: start, prev: b}
		b.init(insert)
		p.tail[string(insert.key)] = b
		return
	}
	for {
		if b.start == start {
			// Update existing bucket
			b.update(insert)
			return
		}
		if b.prev == nil || b.prev.start.Before(start) {
			// Insert new bucket
			p.t.statsMutex.Lock()
			p.t.statsMutex.Unlock()
			b.prev = &bucket{start: start, prev: b.prev}
			b.prev.init(insert)
			return
		}
		// Continue looking
		b = b.prev
	}
}

func (insert *insert) Get(name string) expr.Value {
	// First look in fields
	val, ok := insert.vals[name]
	if ok {
		return val
	}
	return expr.Zero
}

func (p *partition) requestArchiving() {
	now := p.t.clock.Now()
	p.t.log.Tracef("Requested archiving at %v", now)
	for key, b := range p.tail {
		if now.Sub(b.start) > p.t.hotPeriod {
			p.t.log.Tracef("Archiving full. %v / %v %v", b.start, now, b.prev != nil)
			delete(p.tail, key)
			p.t.statsMutex.Lock()
			p.t.stats.HotKeys--
			p.t.statsMutex.Unlock()
			p.t.toArchive <- &archiveRequest{key, b}
			continue
		}
		next := b
		for {
			b = b.prev
			if b == nil {
				break
			}
			p.t.log.Tracef("Checking %v", b.start)
			if now.Sub(b.start) > p.t.hotPeriod {
				p.t.log.Trace("Archiving partial")
				p.t.toArchive <- &archiveRequest{key, b}
				next.prev = nil
				break
			}
		}
	}
}

func (t *table) archive() {
	batch := gorocksdb.NewWriteBatch()
	wo := gorocksdb.NewDefaultWriteOptions()
	for req := range t.toArchive {
		batch = t.doArchive(batch, wo, req)
	}
}

func (t *table) doArchive(batch *gorocksdb.WriteBatch, wo *gorocksdb.WriteOptions, req *archiveRequest) *gorocksdb.WriteBatch {
	key := []byte(req.key)
	seqs := req.b.toSequences(t.resolution)
	numPeriods := int64(seqs[0].numPeriods())
	start := seqs[0].start()
	if t.log.IsTraceEnabled() {
		t.log.Tracef("Archiving %d buckets starting at %v", numPeriods, start.In(time.UTC))
	}
	t.statsMutex.Lock()
	t.stats.ArchivedBuckets += numPeriods
	t.statsMutex.Unlock()
	for i, field := range t.fields {
		k := keyWithField(key, field.Name)
		batch.Merge(k, seqs[i])
	}
	count := int64(batch.Count())
	if count >= t.batchSize {
		err := t.archiveByKey.Write(wo, batch)
		if err != nil {
			t.log.Errorf("Unable to write batch: %v", err)
		}
		batch = gorocksdb.NewWriteBatch()
	}

	return batch
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
	it := t.archiveByKey.NewIterator(ro)
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
		err := t.archiveByKey.Write(wo, batch)
		if err != nil {
			t.log.Errorf("Unable to remove expired keys: %v", err)
		} else {
			delta := time.Now().Sub(start)
			expiredKeys := int64(batch.Count())
			t.statsMutex.Lock()
			t.stats.ExpiredKeys += expiredKeys
			t.statsMutex.Unlock()
			t.log.Tracef("Removed %v expired keys in %v", humanize.Comma(expiredKeys), delta)
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
