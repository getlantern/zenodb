package tdb

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/oxtoacart/tdb/expr"
	"github.com/oxtoacart/tdb/values"
	"github.com/spaolacci/murmur3"
	"github.com/tecbot/gorocksdb"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Point struct {
	Ts   time.Time
	Dims map[string]interface{}
	Vals map[string]float64
}

type Value interface {
	Val() float64

	Add(addend float64) Value
}

type FloatValue float64

func (v FloatValue) Val() float64 {
	return float64(v)
}

func (v FloatValue) Add(addend float64) Value {
	return FloatValue(v.Val() + addend)
}

type bucket struct {
	start time.Time
	val   values.Value
	prev  *bucket
}

type partition struct {
	t       *table
	inserts chan *insert
	tail    map[string]*bucket
}

type insert struct {
	ts  time.Time
	key []byte
	val values.Value
}

type archiveRequest struct {
	key string
	b   *bucket
}

func (db *DB) Insert(table string, point *Point) error {
	t := db.getTable(table)
	if t == nil {
		return fmt.Errorf("Unknown table %v", table)
	}

	return t.insert(point)
}

func (t *table) insert(point *Point) error {
	t.clock.Advance(point.Ts)
	vals := floatsToValues(point.Vals)
	// TODO: deal with situation where name of inserted field conflicts with
	// derived field
	for _, field := range t.derivedFields {
		vals[field.Name] = field.Expr(expr.Map(vals))
	}

	for field, val := range vals {
		key, err := point.keyFor(field)
		if err != nil {
			return err
		}
		h := int(murmur3.Sum32(key))
		p := h % len(t.partitions)
		select {
		case t.partitions[p].inserts <- &insert{point.Ts, key, val}:
			t.statsMutex.Lock()
			t.stats.InsertedPoints++
			t.statsMutex.Unlock()
		default:
			t.statsMutex.Lock()
			t.stats.DroppedPoints++
			t.statsMutex.Unlock()
		}
	}

	return nil
}

func (p *partition) processInserts() {
	archivePeriod := p.t.hotPeriod / 10
	log.Debugf("Archiving every %v", archivePeriod)
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
	key := string(insert.key)
	now := p.t.clock.Now()
	start := roundTime(insert.ts, p.t.resolution)
	if now.Sub(start) > p.t.hotPeriod {
		log.Trace("Discarding insert outside of hot period")
		return
	}
	b := p.tail[key]
	if b == nil || b.start.Before(start) {
		p.t.statsMutex.Lock()
		if b == nil {
			p.t.stats.HotKeys++
		}
		p.t.statsMutex.Unlock()
		b = &bucket{start, insert.val, b}
		p.tail[key] = b
		return
	}
	for {
		if b.start == start {
			// Update existing bucket
			b.val = b.val.Plus(insert.val)
			return
		}
		if b.prev == nil || b.prev.start.Before(start) {
			// Insert new bucket
			p.t.statsMutex.Lock()
			p.t.statsMutex.Unlock()
			b.prev = &bucket{start, insert.val, b.prev}
			return
		}
		// Continue looking
		b = b.prev
	}
}

func (p *partition) requestArchiving() {
	now := p.t.clock.Now()
	log.Debugf("Requested archiving at %v", now)
	for key, b := range p.tail {
		if now.Sub(b.start) > p.t.hotPeriod {
			log.Tracef("Archiving full. %v / %v %v", b.start, now, b.prev != nil)
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
			log.Tracef("Checking %v", b.start)
			if now.Sub(b.start) > p.t.hotPeriod {
				log.Trace("Archiving partial")
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
	seq := req.b.toSequence(t.resolution)
	numBuckets := int64(seq.numBuckets())
	if log.IsTraceEnabled() {
		log.Tracef("Archiving %d buckets starting at %v", numBuckets, seq.start().In(time.UTC))
	}
	t.statsMutex.Lock()
	t.stats.ArchivedBuckets += numBuckets
	t.statsMutex.Unlock()
	batch.Merge(key, seq)
	count := int64(batch.Count())
	if count >= t.batchSize {
		err := t.archiveByKey.Write(wo, batch)
		if err != nil {
			log.Errorf("Unable to write batch: %v", err)
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
	log.Debug("Removing expired keys")
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
			log.Errorf("Unable to remove expired keys: %v", err)
		} else {
			delta := time.Now().Sub(start)
			log.Debugf("Removed %v expired keys in %v", humanize.Comma(int64(batch.Count())), delta)
		}
	} else {
		log.Debug("No expired keys to remove")
	}
}

func (p *Point) keyFor(field string) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := msgpack.NewEncoder(buf)
	enc.SortMapKeys(true)
	err := enc.Encode(field)
	if err != nil {
		return nil, fmt.Errorf("Unable to encode field: %v", err)
	}
	err = enc.Encode(p.Dims)
	if err != nil {
		return nil, fmt.Errorf("Unable to encode dims: %v", err)
	}
	return buf.Bytes(), nil
}

func floatsToValues(in map[string]float64) map[string]values.Value {
	out := make(map[string]values.Value, len(in))
	for key, value := range in {
		out[key] = values.Float(value)
	}
	return out
}
