package tdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
	"github.com/golang/snappy"
)

type columnStoreOptions struct {
	dir            string
	ex             expr.Expr
	resolution     time.Duration
	truncateBefore func() time.Time
	numMemStores   int
	flushAt        int
}

type columnStore struct {
	opts          *columnStoreOptions
	memStores     []memStore
	fileStore     *fileStore
	inserts       chan *insert
	flushes       chan memStore
	flushFinished chan memStore
	mx            sync.RWMutex
}

func openColumnStore(opts *columnStoreOptions) (*columnStore, error) {
	if opts.numMemStores == 0 {
		opts.numMemStores = 2
	}
	if opts.flushAt == 0 {
		opts.flushAt = 1000
	}

	err := os.MkdirAll(opts.dir, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("Unable to create folder for column store: %v", err)
	}

	cs := &columnStore{
		opts:          opts,
		memStores:     make([]memStore, 0, opts.numMemStores),
		inserts:       make(chan *insert),
		flushes:       make(chan memStore, opts.numMemStores),
		flushFinished: make(chan memStore, 1),
	}
	cs.fileStore = &fileStore{
		cs: cs,
	}

	go cs.processInserts()
	for i := 0; i < runtime.NumCPU(); i++ {
		go cs.processFlushes()
	}

	return cs, nil
}

func (cs *columnStore) insert(insert *insert) {
	cs.inserts <- insert
}

func (cs *columnStore) iterate(onValue func(bytemap.ByteMap, sequence)) error {
	cs.mx.RLock()
	fs := cs.fileStore
	memStores := cs.memStores
	cs.mx.RUnlock()
	memStoresCopy := make([]memStore, 0, len(memStores))
	for _, ms := range memStores {
		msCopy := make(map[string]sequence, len(ms))
		for key, seq := range ms {
			msCopy[key] = seq
		}
		memStoresCopy = append(memStoresCopy, msCopy)
	}
	return fs.iterate(memStoresCopy, onValue)
}

func (cs *columnStore) processInserts() {
	currentMemStore := make(memStore, cs.opts.flushAt)
	cs.memStores = append(cs.memStores, currentMemStore)
	accum := cs.opts.ex.Accumulator()
	for {
		select {
		case insert := <-cs.inserts:
			currentMemStore[insert.key] = currentMemStore[insert.key].update(insert.vals, accum, cs.opts.resolution, cs.opts.truncateBefore())
		}
	}
}

func (cs *columnStore) processFlushes() {

}

type memStore map[string]sequence

func (ms memStore) remove(key string) sequence {
	seq, found := ms[key]
	if found {
		delete(ms, key)
	}
	return seq
}

type fileStore struct {
	cs       *columnStore
	filename string
}

func (fs *fileStore) iterate(memStores []memStore, onValue func(bytemap.ByteMap, sequence)) error {
	accum1 := fs.cs.opts.ex.Accumulator()
	accum2 := fs.cs.opts.ex.Accumulator()

	file, err := os.OpenFile(fs.filename, os.O_RDONLY, 0)
	if !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("Unable to open file %v: %v", fs.filename, err)
		}
		r := snappy.NewReader(file)

		// Read from file
		for {
			keyLength := int16(0)
			err := binary.Read(file, binaryEncoding, &keyLength)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("Unexpected error reading key length: %v", err)
			}
			seqLength := int64(0)
			err = binary.Read(file, binaryEncoding, &seqLength)
			if err != nil {
				return fmt.Errorf("Unexpected error reading sequence length: %v", err)
			}
			key := make(bytemap.ByteMap, keyLength)
			seq := make(sequence, seqLength)
			_, err = io.ReadFull(r, key)
			if err != nil {
				return fmt.Errorf("Unexpected error reading key: %v", err)
			}
			_, err = io.ReadFull(r, seq)
			if err != nil {
				return fmt.Errorf("Unexpected error reading seq: %v", err)
			}
			for _, ms := range memStores {
				seq = seq.merge(ms.remove(string(key)), fs.cs.opts.resolution, accum1, accum2)
			}
			onValue(key, seq)
		}
	}

	// Read remaining stuff from mem stores
	for i, ms := range memStores {
		for key, seq := range ms {
			for j := i + 1; j < len(memStores); j++ {
				ms2 := memStores[j]
				seq = seq.merge(ms2.remove(string(key)), fs.cs.opts.resolution, accum1, accum2)
			}
			onValue(bytemap.ByteMap(key), seq)
		}
	}

	return nil
}
