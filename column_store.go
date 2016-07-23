package tdb

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/tdb/expr"
	"github.com/golang/snappy"
)

// TODO: read existing filestore on startup (use one with most recent date)
// TODO: add WAL

type columnStoreOptions struct {
	dir              string
	ex               expr.Expr
	resolution       time.Duration
	truncateBefore   func() time.Time
	maxMemStoreBytes int
	maxFlushLatency  time.Duration
}

type flushRequest struct {
	idx int
	ms  memStore
}

type flushResponse struct {
	idx              int
	newFileStoreName string
	duration         time.Duration
}

type columnStore struct {
	opts          *columnStoreOptions
	memStores     map[int]memStore
	fileStore     *fileStore
	inserts       chan *insert
	flushes       chan *flushRequest
	flushFinished chan *flushResponse
	mx            sync.RWMutex
}

func openColumnStore(opts *columnStoreOptions) (*columnStore, error) {
	err := os.MkdirAll(opts.dir, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("Unable to create folder for column store: %v", err)
	}

	cs := &columnStore{
		opts:          opts,
		memStores:     make(map[int]memStore, 2),
		inserts:       make(chan *insert),
		flushes:       make(chan *flushRequest, 1),
		flushFinished: make(chan *flushResponse, 1),
	}
	cs.fileStore = &fileStore{
		cs: cs,
	}

	go cs.processInserts()
	go cs.processFlushes()

	return cs, nil
}

func (cs *columnStore) insert(insert *insert) {
	cs.inserts <- insert
}

func (cs *columnStore) processInserts() {
	memStoreIdx := 0
	memStoreBytes := 0
	currentMemStore := make(memStore)
	cs.memStores[memStoreIdx] = currentMemStore

	flushInterval := cs.opts.maxFlushLatency

	flush := func() {
		if memStoreBytes == 0 {
			// nothing to flush
			return
		}
		log.Debugf("Requesting flush at memstore size: %v", humanize.Bytes(uint64(memStoreBytes)))
		memStoreCopy := currentMemStore.copy()
		cs.mx.Lock()
		currentMemStore = make(memStore, len(currentMemStore))
		memStoreIdx++
		cs.memStores[memStoreIdx] = currentMemStore
		memStoreBytes = 0
		cs.mx.Unlock()
		cs.flushes <- &flushRequest{memStoreIdx, memStoreCopy}
		log.Debug("Requested flush")
	}

	flushTimer := time.NewTimer(flushInterval)

	for {
		select {
		case insert := <-cs.inserts:
			current := currentMemStore[insert.key]
			previousSize := len(current)
			if current == nil {
				memStoreBytes += len(insert.key)
			}
			cs.mx.Lock()
			updated := current.update(insert.vals, cs.opts.ex, cs.opts.resolution, cs.opts.truncateBefore())
			currentMemStore[insert.key] = updated
			cs.mx.Unlock()
			memStoreBytes += len(updated) - previousSize
			if memStoreBytes >= cs.opts.maxMemStoreBytes {
				flush()
			}
		case <-flushTimer.C:
			flush()
		case fr := <-cs.flushFinished:
			oldFileStore := cs.fileStore.filename
			cs.mx.Lock()
			delete(cs.memStores, fr.idx)
			cs.fileStore = &fileStore{cs, fr.newFileStoreName}
			if oldFileStore != "" {
				err := os.Remove(oldFileStore)
				if err != nil {
					log.Errorf("Unable to delete old file store, still consuming disk space unnecessarily: %v", err)
				}
			}
			cs.mx.Unlock()
			flushTimer.Reset(fr.duration * 2)
		}
	}
}

func (cs *columnStore) iterate(onValue func(bytemap.ByteMap, sequence)) error {
	cs.mx.RLock()
	fs := cs.fileStore
	memStoresCopy := make([]memStore, 0, len(cs.memStores))
	for _, ms := range cs.memStores {
		memStoresCopy = append(memStoresCopy, ms.copy())
	}
	cs.mx.RUnlock()
	return fs.iterate(onValue, memStoresCopy...)
}

func (cs *columnStore) processFlushes() {
	for req := range cs.flushes {
		start := time.Now()
		out, err := ioutil.TempFile("", "nextcolumnstore")
		if err != nil {
			panic(err)
		}
		sout := snappy.NewWriter(out)
		cout := bufio.NewWriterSize(sout, 65536)

		b := make([]byte, 8)
		write := func(key bytemap.ByteMap, seq sequence) {
			binaryEncoding.PutUint16(b, uint16(len(key)))
			_, err := cout.Write(b[:2])
			if err != nil {
				panic(err)
			}
			binaryEncoding.PutUint64(b, uint64(len(seq)))
			_, err = cout.Write(b)
			if err != nil {
				panic(err)
			}
			_, err = cout.Write(key)
			if err != nil {
				panic(err)
			}
			_, err = cout.Write(seq)
			if err != nil {
				panic(err)
			}
		}
		cs.mx.RLock()
		fs := cs.fileStore
		cs.mx.RUnlock()
		fs.iterate(write, req.ms)
		err = cout.Flush()
		if err != nil {
			panic(err)
		}
		err = sout.Close()
		if err != nil {
			panic(err)
		}
		// Note - we left-pad the unix nano value to the widest possible length to
		// ensure lexicographical sort matches time-based sort (e.g. on directory
		// listing).
		newFileStoreName := filepath.Join(cs.opts.dir, fmt.Sprintf("filestore_%020d.dat", time.Now().UnixNano()))
		err = os.Rename(out.Name(), newFileStoreName)
		if err != nil {
			panic(err)
		}
		delta := time.Now().Sub(start)
		cs.flushFinished <- &flushResponse{req.idx, newFileStoreName, delta}
		log.Debugf("Flushed in %v", delta)
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

func (fs *fileStore) iterate(onValue func(bytemap.ByteMap, sequence), memStores ...memStore) error {
	file, err := os.OpenFile(fs.filename, os.O_RDONLY, 0)
	if !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("Unable to open file %v: %v", fs.filename, err)
		}
		r := snappy.NewReader(bufio.NewReaderSize(file, 65536))

		// Read from file
		b := make([]byte, 10)
		for {
			_, err := io.ReadFull(r, b)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("Unexpected error reading lengths: %v", err)
			}
			keyLength := binaryEncoding.Uint16(b)
			seqLength := binaryEncoding.Uint16(b[2:])
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
				before := seq
				seq2 := ms.remove(string(key))
				seq = seq.merge(seq2, fs.cs.opts.resolution, fs.cs.opts.ex)
				log.Debugf("File Merged: %v + %v -> %v", before.String(fs.cs.opts.ex), seq2.String(fs.cs.opts.ex), seq.String(fs.cs.opts.ex))
			}
			onValue(key, seq)
		}
	}

	// Read remaining stuff from mem stores
	for i, ms := range memStores {
		for key, seq := range ms {
			for j := i + 1; j < len(memStores); j++ {
				ms2 := memStores[j]
				before := seq
				seq2 := ms2.remove(string(key))
				seq = seq.merge(seq2, fs.cs.opts.resolution, fs.cs.opts.ex)
				log.Debugf("Mem Merged: %v + %v -> %v", before.String(fs.cs.opts.ex), seq2.String(fs.cs.opts.ex), seq.String(fs.cs.opts.ex))
			}
			onValue(bytemap.ByteMap(key), seq)
		}
	}

	return nil
}
