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
	dir              string
	ex               expr.Expr
	resolution       time.Duration
	truncateBefore   func() time.Time
	numMemStores     int
	maxMemStoreBytes int
	maxFlushLatency  time.Duration
}

type flushRequest struct {
	idx int
	ms  memStore
}

type columnStore struct {
	opts          *columnStoreOptions
	memStores     map[int]memStore
	fileStore     *fileStore
	inserts       chan *insert
	flushes       chan *flushRequest
	flushFinished chan int
	mx            sync.RWMutex
}

func openColumnStore(opts *columnStoreOptions) (*columnStore, error) {
	if opts.numMemStores == 0 {
		opts.numMemStores = runtime.NumCPU()
	}

	err := os.MkdirAll(opts.dir, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("Unable to create folder for column store: %v", err)
	}

	cs := &columnStore{
		opts:          opts,
		memStores:     make(map[int]memStore, opts.numMemStores),
		inserts:       make(chan *insert),
		flushes:       make(chan *flushRequest, opts.numMemStores-1),
		flushFinished: make(chan int, opts.numMemStores),
	}
	cs.fileStore = &fileStore{
		cs: cs,
	}

	go cs.processInserts()
	for i := 0; i < runtime.NumCPU() && i < opts.numMemStores; i++ {
		go cs.processFlushes()
	}

	return cs, nil
}

func (cs *columnStore) insert(insert *insert) {
	cs.inserts <- insert
}

func (cs *columnStore) processInserts() {
	memStoreIdx := 0
	currentMemStore := make(memStore)
	cs.memStores[memStoreIdx] = currentMemStore
	accum := cs.opts.ex.Accumulator()

	flush := func() {
		cs.flushes <- &flushRequest{memStoreIdx, currentMemStore.copy()}
		currentMemStore = make(memStore, len(currentMemStore))
		memStoreIdx++
		cs.memStores[memStoreIdx] = currentMemStore
	}

	flushTicker := time.NewTicker(cs.opts.maxFlushLatency)

	memStoreBytes := 0
	for {
		select {
		case insert := <-cs.inserts:
			current := currentMemStore[insert.key]
			previousSize := len(current)
			if current == nil {
				memStoreBytes += len(insert.key)
			}
			cs.mx.Lock()
			updated := current.update(insert.vals, accum, cs.opts.resolution, cs.opts.truncateBefore())
			currentMemStore[insert.key] = updated
			cs.mx.Unlock()
			memStoreBytes += len(updated) - previousSize
			if memStoreBytes >= cs.opts.maxMemStoreBytes {
				flush()
			}
		case <-flushTicker.C:
			flush()
		case idx := <-cs.flushFinished:
			cs.mx.Lock()
			if false {
				// TODO: enable
				delete(cs.memStores, idx)
			}
			cs.mx.Unlock()
		}
	}
}

func (cs *columnStore) iterate(onValue func(bytemap.ByteMap, sequence)) error {
	cs.mx.RLock()
	fs := cs.fileStore
	memStores := cs.memStores
	cs.mx.RUnlock()
	memStoresCopy := make([]memStore, 0, len(memStores))
	for _, ms := range memStores {
		memStoresCopy = append(memStoresCopy, ms.copy())
	}
	return fs.iterate(memStoresCopy, onValue)
}

func (cs *columnStore) processFlushes() {
	for req := range cs.flushes {
		// TODO: write it to a file
		cs.flushFinished <- req.idx
	}
}

type memStore map[string]sequence

func (ms memStore) remove(key string) sequence {
	seq, found := ms[key]
	if found {
		delete(ms, key)
	}
	return seq
}

func (ms memStore) copy() memStore {
	memStoreCopy := make(map[string]sequence, len(ms))
	for key, seq := range ms {
		memStoreCopy[key] = seq
	}
	return memStoreCopy
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
