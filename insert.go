package tdb

import (
	"bytes"
	"fmt"
	"time"

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

type bucket struct {
	start time.Time
	vals  map[string]values.Value
	prev  *bucket
}

type partition struct {
	t       *table
	inserts chan *insert
	tail    map[string]*bucket
}

type insert struct {
	ts   time.Time
	key  []byte
	vals map[string]float64
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

	// TODO: deal with situation name of inserted field conflicts with derived
	// field
	return t.insert(point)
}

func (t *table) insert(point *Point) error {
	t.clock.Advance(point.Ts)
	insert, err := point.asInsert()
	if err != nil {
		return err
	}
	h := int(murmur3.Sum32(insert.key))
	p := h % len(t.partitions)
	t.partitions[p].inserts <- insert
	return nil
}

func (p *partition) processInserts() {
	// TODO: base this on the passage of fake time
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
		b = &bucket{start, floatsToValues(insert.vals), b}
		p.tail[key] = b
		return
	}
	for {
		if b.start == start {
			// Update existing bucket
			for key, val := range insert.vals {
				b.vals[key] = b.vals[key].Add(val)
			}
			return
		}
		if b.prev == nil || b.prev.start.Before(start) {
			// Insert new bucket
			p.t.statsMutex.Lock()
			p.t.statsMutex.Unlock()
			b.prev = &bucket{start, floatsToValues(insert.vals), b.prev}
			return
		}
		// Continue looking
		b = b.prev
	}
}

func (p *partition) requestArchiving() {
	now := p.t.clock.Now()
	log.Tracef("Requested archiving at %v", now)
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
	wo := gorocksdb.NewDefaultWriteOptions()

	batch := gorocksdb.NewWriteBatch()
	for req := range t.toArchive {
		seqs := req.b.toSequences(t.resolution, t.derivedFields...)
		for field, seq := range seqs {
			keyBytes := []byte(req.key)
			keyBuf := bytes.NewBuffer(make([]byte, 0, len(keyBytes)))
			err := msgpack.NewEncoder(keyBuf).EncodeString(field)
			if err != nil {
				log.Errorf("Unable to encode field: %v", err)
				continue
			}
			_, err = keyBuf.Write(keyBytes)
			if err != nil {
				log.Errorf("Unable to write key: %v", err)
				continue
			}
			numBuckets := int64(seq.numBuckets())
			if log.IsTraceEnabled() {
				log.Tracef("Archiving %d buckets for %v starting at %v", numBuckets, field, seq.start().In(time.UTC))
			}
			batch.Merge(keyBuf.Bytes(), seq)
			t.stats.ArchivedBuckets += numBuckets
		}
		count := int64(batch.Count())
		if count >= t.batchSize {
			err := t.archiveByKey.Write(wo, batch)
			if err != nil {
				log.Errorf("Unable to write batch: %v", err)
			}
			t.statsMutex.Lock()
			t.statsMutex.Unlock()
			batch = gorocksdb.NewWriteBatch()
		}
	}
}

func floatsToValues(in map[string]float64) map[string]values.Value {
	out := make(map[string]values.Value, len(in))
	for key, value := range in {
		out[key] = values.Float(value)
	}
	return out
}

func (p *Point) asInsert() (*insert, error) {
	key, err := p.key()
	if err != nil {
		return nil, err
	}
	return &insert{p.Ts, key, p.Vals}, nil
}

func (p *Point) key() ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := msgpack.NewEncoder(buf)
	enc.SortMapKeys(true)
	err := enc.Encode(p.Dims)
	if err != nil {
		return nil, fmt.Errorf("Unable to encode dims: %v", err)
	}
	return buf.Bytes(), nil
}
