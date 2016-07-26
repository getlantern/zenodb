package tdb

import (
	"bufio"
	"bytes"
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
	"github.com/oxtoacart/emsort"
)

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
	idx  int
	ms   memStore
	sort bool
}

type columnStore struct {
	opts          *columnStoreOptions
	memStores     map[int]memStore
	fileStore     *fileStore
	inserts       chan *insert
	flushes       chan *flushRequest
	flushFinished chan time.Duration
	mx            sync.RWMutex
}

func openColumnStore(opts *columnStoreOptions) (*columnStore, error) {
	err := os.MkdirAll(opts.dir, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("Unable to create folder for column store: %v", err)
	}

	existingFileName := ""
	files, err := ioutil.ReadDir(opts.dir)
	if err != nil {
		return nil, fmt.Errorf("Unable to read contents of directory: %v", err)
	}
	if len(files) > 0 {
		existingFileName = filepath.Join(opts.dir, files[len(files)-1].Name())
		log.Debugf("Initializing column store from %v", existingFileName)
	}

	cs := &columnStore{
		opts:          opts,
		memStores:     make(map[int]memStore, 2),
		inserts:       make(chan *insert),
		flushes:       make(chan *flushRequest, 1),
		flushFinished: make(chan time.Duration, 1),
		fileStore: &fileStore{
			opts:     opts,
			filename: existingFileName,
		},
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
	flushIdx := 0
	flush := func() {
		if memStoreBytes == 0 {
			// nothing to flush
			return
		}
		log.Debugf("Requesting flush at memstore size: %v", humanize.Bytes(uint64(memStoreBytes)))
		memStoreCopy := currentMemStore.copy()
		shouldSort := flushIdx%10 == 0
		fr := &flushRequest{memStoreIdx, memStoreCopy, shouldSort}
		cs.mx.Lock()
		flushIdx++
		currentMemStore = make(memStore, len(currentMemStore))
		memStoreIdx++
		cs.memStores[memStoreIdx] = currentMemStore
		memStoreBytes = 0
		cs.mx.Unlock()
		cs.flushes <- fr
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
		case flushDuration := <-cs.flushFinished:
			flushTimer.Reset(flushDuration * 10)
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

		if req.sort {
			sd := &sortData{cs, req.ms, cout}
			err = emsort.Sorted(sd, cs.opts.maxMemStoreBytes/2)
			if err != nil {
				panic(fmt.Errorf("Unable to process flush: %v", err))
			}
		} else {
			// TODO: DRY violation with sortData.Fill sortData.OnSorted
			periodWidth := cs.opts.ex.EncodedWidth()
			truncateBefore := cs.opts.truncateBefore()
			b := make([]byte, width16bits+width64bits)
			write := func(key bytemap.ByteMap, seq sequence) {
				seq = seq.truncate(periodWidth, cs.opts.resolution, truncateBefore)
				if seq == nil {
					// entire sequence is expired, remove it
					return
				}
				binaryEncoding.PutUint16(b, uint16(len(key)))
				binaryEncoding.PutUint64(b[width16bits:], uint64(len(seq)))
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
		}
		err = cout.Flush()
		if err != nil {
			panic(err)
		}
		err = sout.Close()
		if err != nil {
			panic(err)
		}

		fi, err := out.Stat()
		if err != nil {
			log.Errorf("Unable to stat output file to get size: %v", err)
		}
		// Note - we left-pad the unix nano value to the widest possible length to
		// ensure lexicographical sort matches time-based sort (e.g. on directory
		// listing).
		newFileStoreName := filepath.Join(cs.opts.dir, fmt.Sprintf("filestore_%020d.dat", time.Now().UnixNano()))
		err = os.Rename(out.Name(), newFileStoreName)
		if err != nil {
			panic(err)
		}

		oldFileStore := cs.fileStore.filename
		cs.mx.Lock()
		delete(cs.memStores, req.idx)
		cs.fileStore = &fileStore{cs.opts, newFileStoreName}
		cs.mx.Unlock()

		// TODO: add background process for cleaning up old file stores
		if oldFileStore != "" {
			go func() {
				time.Sleep(5 * time.Minute)
				err := os.Remove(oldFileStore)
				if err != nil {
					log.Errorf("Unable to delete old file store, still consuming disk space unnecessarily: %v", err)
				}
			}()
		}

		flushDuration := time.Now().Sub(start)
		cs.flushFinished <- flushDuration
		wasSorted := "not sorted"
		if req.sort {
			wasSorted = "sorted"
		}
		if fi != nil {
			log.Debugf("Flushed to %v in %v, size %v. %v.", newFileStoreName, flushDuration, humanize.Bytes(uint64(fi.Size())), wasSorted)
		} else {
			log.Debugf("Flushed to %v in %v. %v.", newFileStoreName, flushDuration, wasSorted)
		}
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
	opts     *columnStoreOptions
	filename string
}

func (fs *fileStore) iterate(onValue func(bytemap.ByteMap, sequence), memStores ...memStore) error {
	if log.IsTraceEnabled() {
		log.Tracef("Iterating with %d memstores from file %v", len(memStores), fs.filename)
	}

	truncateBefore := fs.opts.truncateBefore()
	file, err := os.OpenFile(fs.filename, os.O_RDONLY, 0)
	if !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("Unable to open file %v: %v", fs.filename, err)
		}
		r := snappy.NewReader(bufio.NewReaderSize(file, 65536))

		// Read from file
		b := make([]byte, width16bits+width64bits)
		for {
			_, err := io.ReadFull(r, b)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("Unexpected error reading lengths: %v", err)
			}
			keyLength := binaryEncoding.Uint16(b)
			seqLength := binaryEncoding.Uint64(b[width16bits:])
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
			if log.IsTraceEnabled() {
				log.Tracef("File Read: %v", seq.String(fs.opts.ex))
			}
			for _, ms := range memStores {
				before := seq
				seq2 := ms.remove(string(key))
				seq = seq.merge(seq2, fs.opts.ex, fs.opts.resolution, truncateBefore)
				if log.IsTraceEnabled() {
					log.Tracef("File Merged: %v + %v -> %v", before.String(fs.opts.ex), seq2.String(fs.opts.ex), seq.String(fs.opts.ex))
				}
			}
			onValue(key, seq)
		}
	}

	// Read remaining stuff from mem stores
	for i, ms := range memStores {
		for key, seq := range ms {
			if log.IsTraceEnabled() {
				log.Tracef("Mem Read: %v", seq.String(fs.opts.ex))
			}
			for j := i + 1; j < len(memStores); j++ {
				ms2 := memStores[j]
				before := seq
				seq2 := ms2.remove(string(key))
				seq = seq.merge(seq2, fs.opts.ex, fs.opts.resolution, truncateBefore)
				if log.IsTraceEnabled() {
					log.Tracef("Mem Merged: %v + %v -> %v", before.String(fs.opts.ex), seq2.String(fs.opts.ex), seq.String(fs.opts.ex))
				}
			}
			onValue(bytemap.ByteMap(key), seq)
		}
	}

	return nil
}

type sortData struct {
	cs  *columnStore
	ms  memStore
	out io.Writer
}

func (sd *sortData) Fill(fn func([]byte) error) error {
	periodWidth := sd.cs.opts.ex.EncodedWidth()
	truncateBefore := sd.cs.opts.truncateBefore()
	doFill := func(key bytemap.ByteMap, seq sequence) {
		seq = seq.truncate(periodWidth, sd.cs.opts.resolution, truncateBefore)
		if seq == nil {
			// entire sequence is expired, remove it
			return
		}
		b := make([]byte, width16bits+width64bits+len(key)+len(seq))
		binaryEncoding.PutUint16(b, uint16(len(key)))
		binaryEncoding.PutUint64(b[width16bits:], uint64(len(seq)))
		copy(b[width16bits+width64bits:], key)
		copy(b[width16bits+width64bits+len(key):], seq)
		fn(b)
	}
	sd.cs.mx.RLock()
	fs := sd.cs.fileStore
	sd.cs.mx.RUnlock()
	fs.iterate(doFill, sd.ms)
	return nil
}

func (sd *sortData) Read(r io.Reader) ([]byte, error) {
	b := make([]byte, width16bits+width64bits)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}
	keyLength := binaryEncoding.Uint16(b)
	seqLength := binaryEncoding.Uint64(b[width16bits:])
	b2 := make([]byte, len(b)+int(keyLength)+int(seqLength))
	_b2 := b2
	copy(_b2, b)
	_b2 = _b2[width16bits+width64bits:]
	_, err = io.ReadFull(r, _b2)
	if err != nil {
		return nil, err
	}
	return b2, nil
}

func (sd *sortData) Less(a []byte, b []byte) bool {
	// We compare key/value pairs by doing a lexicographical comparison on the
	// longest portion of the key that's available in both values.
	keyLength := binaryEncoding.Uint16(a)
	bKeyLength := binaryEncoding.Uint16(b)
	if bKeyLength < keyLength {
		keyLength = bKeyLength
	}
	s := width16bits + width64bits // exclude key and seq length header
	e := s + int(keyLength)
	return bytes.Compare(a[s:e], b[s:e]) < 0
}

func (sd *sortData) OnSorted(b []byte) error {
	_, err := sd.out.Write(b)
	return err
}
