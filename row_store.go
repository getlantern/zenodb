package zenodb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/bytetree"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/sql"
	"github.com/golang/snappy"
	"github.com/oxtoacart/emsort"
)

// TODO: add WAL

const (
	// File format versions
	FileVersion_2      = 2
	CurrentFileVersion = FileVersion_2
)

type rowStoreOptions struct {
	dir              string
	maxMemStoreBytes int
	minFlushLatency  time.Duration
	maxFlushLatency  time.Duration
}

type flushRequest struct {
	idx      int
	memstore *bytetree.Tree
	sort     bool
}

type rowStore struct {
	t                  *table
	opts               *rowStoreOptions
	memStores          map[int]*bytetree.Tree
	currentMemStoreIdx int
	fileStore          *fileStore
	inserts            chan *insert
	flushes            chan *flushRequest
	flushFinished      chan time.Duration
	mx                 sync.RWMutex
}

func (t *table) openRowStore(opts *rowStoreOptions) (*rowStore, error) {
	err := os.MkdirAll(opts.dir, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("Unable to create folder for row store: %v", err)
	}

	existingFileName := ""
	files, err := ioutil.ReadDir(opts.dir)
	if err != nil {
		return nil, fmt.Errorf("Unable to read contents of directory: %v", err)
	}
	if len(files) > 0 {
		// files are sorted by name, in our case timestamp, so the last file in the
		// list is the most recent.  That's the one that we want.
		existingFileName = filepath.Join(opts.dir, files[len(files)-1].Name())
		t.log.Debugf("Initializing row store from %v", existingFileName)
	}

	rs := &rowStore{
		opts:               opts,
		t:                  t,
		memStores:          make(map[int]*bytetree.Tree, 2),
		currentMemStoreIdx: 0,
		inserts:            make(chan *insert),
		flushes:            make(chan *flushRequest, 1),
		flushFinished:      make(chan time.Duration, 1),
		fileStore: &fileStore{
			t:        t,
			opts:     opts,
			filename: existingFileName,
		},
	}

	go rs.processInserts()
	go rs.processFlushes()
	go rs.removeOldFiles()

	return rs, nil
}

func (rs *rowStore) insert(insert *insert) {
	rs.inserts <- insert
}

func (rs *rowStore) processInserts() {
	currentMemStore := bytetree.New()
	rs.mx.Lock()
	rs.memStores[rs.currentMemStoreIdx] = currentMemStore
	rs.mx.Unlock()

	flushInterval := rs.opts.maxFlushLatency
	flushTimer := time.NewTimer(flushInterval)
	rs.t.log.Debugf("Will flush after %v", flushInterval)

	flushIdx := 0
	flush := func() {
		if currentMemStore.Length() == 0 {
			rs.t.log.Trace("Nothing to flush")
			// Immediately reset flushTimer
			flushTimer.Reset(flushInterval)
			return
		}
		// Temporarily disable flush timer while we're flushing
		flushTimer.Reset(100000 * time.Hour)
		rs.t.log.Debugf("Requesting flush at memstore size: %v", humanize.Bytes(uint64(currentMemStore.Bytes())))
		previousMemStore := currentMemStore
		shouldSort := flushIdx%10 == 0
		rs.mx.Lock()
		fr := &flushRequest{rs.currentMemStoreIdx, previousMemStore, shouldSort}
		rs.mx.Unlock()
		select {
		case rs.flushes <- fr:
			rs.mx.Lock()
			currentMemStore = bytetree.New()
			rs.currentMemStoreIdx++
			rs.memStores[rs.currentMemStoreIdx] = currentMemStore
			flushIdx++
			rs.mx.Unlock()
		default:
			log.Debug("Unable to post flush request, will try again later")
		}
	}

	for {
		select {
		case insert := <-rs.inserts:
			truncateBefore := rs.t.truncateBefore()
			rs.mx.Lock()
			currentMemStore.Update(rs.t.Fields, rs.t.Resolution, truncateBefore, insert.key, insert.vals)
			rs.mx.Unlock()
			if currentMemStore.Bytes() >= rs.opts.maxMemStoreBytes {
				flush()
			}
		case <-flushTimer.C:
			flush()
		case flushDuration := <-rs.flushFinished:
			flushInterval = flushDuration * 10
			if flushInterval > rs.opts.maxFlushLatency {
				flushInterval = rs.opts.maxFlushLatency
			} else if flushInterval < rs.opts.minFlushLatency {
				flushInterval = rs.opts.minFlushLatency
			}
			flushTimer.Reset(flushInterval)
		}
	}
}

func (rs *rowStore) iterate(fields []string, onValue func(bytemap.ByteMap, []encoding.Sequence)) error {
	rs.mx.RLock()
	fs := rs.fileStore
	memStoresCopy := make([]*bytetree.Tree, 0, len(rs.memStores))
	for i, ms := range rs.memStores {
		onCurrentMemStore := i == rs.currentMemStoreIdx
		if onCurrentMemStore {
			// Current memstore is still getting writes.  Either omit, or copy.
			if !rs.t.db.opts.IncludeMemStoreInQuery {
				// omit
				continue
			}
			// copy
			ms = ms.Copy()
		}
		memStoresCopy = append(memStoresCopy, ms)
	}
	rs.mx.RUnlock()
	return fs.iterate(onValue, memStoresCopy, fields...)
}

func (rs *rowStore) processFlushes() {
	for req := range rs.flushes {
		start := time.Now()
		out, err := ioutil.TempFile("", "nextrowstore")
		if err != nil {
			panic(err)
		}
		sout := snappy.NewWriter(out)
		bout := bufio.NewWriterSize(sout, 65536)

		// Write header with field strings
		fieldStrings := make([]string, 0, len(rs.t.Fields))
		for _, field := range rs.t.Fields {
			fieldStrings = append(fieldStrings, field.String())
		}
		headerBytes := []byte(strings.Join(fieldStrings, ","))
		headerLength := uint32(len(headerBytes))
		err = binary.Write(bout, encoding.Binary, headerLength)
		if err != nil {
			panic(fmt.Errorf("Unable to write header length: %v", err))
		}
		_, err = bout.Write(headerBytes)
		if err != nil {
			panic(fmt.Errorf("Unable to write header: %v", err))
		}

		var cout io.WriteCloser
		if !req.sort {
			cout = &closerAdapter{bout}
		} else {
			chunk := func(r io.Reader) ([]byte, error) {
				rowLength := uint64(0)
				readErr := binary.Read(r, encoding.Binary, &rowLength)
				if readErr != nil {
					return nil, readErr
				}
				_row := make([]byte, rowLength)
				row := _row
				encoding.Binary.PutUint64(row, rowLength)
				row = row[encoding.Width64bits:]
				_, err = io.ReadFull(r, row)
				return _row, err
			}

			less := func(a []byte, b []byte) bool {
				return bytes.Compare(a, b) < 0
			}

			var sortErr error
			cout, sortErr = emsort.New(bout, chunk, less, rs.opts.maxMemStoreBytes/2)
			if sortErr != nil {
				panic(sortErr)
			}
		}

		truncateBefore := rs.t.truncateBefore()
		write := func(key bytemap.ByteMap, columns []encoding.Sequence) {
			hasActiveSequence := false
			for i, seq := range columns {
				seq = seq.Truncate(rs.t.Fields[i].Expr.EncodedWidth(), rs.t.Resolution, truncateBefore)
				columns[i] = seq
				if seq != nil {
					hasActiveSequence = true
				}
			}

			if !hasActiveSequence {
				// all encoding.Sequences expired, remove key
				return
			}

			rowLength := encoding.Width64bits + encoding.Width16bits + len(key) + encoding.Width16bits
			for _, seq := range columns {
				rowLength += encoding.Width64bits + len(seq)
			}

			var o io.Writer = cout
			var buf *bytes.Buffer
			if req.sort {
				// When sorting, we need to write the entire row as a single byte array,
				// so use a ByteBuffer. We don't do this otherwise because we're already
				// using a buffered writer, so we can avoid the copying
				b := make([]byte, 0, rowLength)
				buf = bytes.NewBuffer(b)
				o = buf
			}

			err = binary.Write(o, encoding.Binary, uint64(rowLength))
			if err != nil {
				panic(err)
			}

			err = binary.Write(o, encoding.Binary, uint16(len(key)))
			if err != nil {
				panic(err)
			}
			_, err = o.Write(key)
			if err != nil {
				panic(err)
			}

			err = binary.Write(o, encoding.Binary, uint16(len(columns)))
			if err != nil {
				panic(err)
			}
			for _, seq := range columns {
				err = binary.Write(o, encoding.Binary, uint64(len(seq)))
				if err != nil {
					panic(err)
				}
			}
			for _, seq := range columns {
				_, err = o.Write(seq)
				if err != nil {
					panic(err)
				}
			}

			if req.sort {
				// flush buffer
				_b := buf.Bytes()
				_, writeErr := cout.Write(_b)
				if writeErr != nil {
					panic(writeErr)
				}
			}
		}
		rs.mx.RLock()
		fs := rs.fileStore
		rs.mx.RUnlock()
		fs.iterate(write, []*bytetree.Tree{req.memstore})
		err = cout.Close()
		if err != nil {
			panic(err)
		}
		err = bout.Flush()
		if err != nil {
			panic(err)
		}
		err = sout.Close()
		if err != nil {
			panic(err)
		}

		fi, err := out.Stat()
		if err != nil {
			rs.t.log.Errorf("Unable to stat output file to get size: %v", err)
		}
		// Note - we left-pad the unix nano value to the widest possible length to
		// ensure lexicographical sort matches time-based sort (e.g. on directory
		// listing).
		newFileStoreName := filepath.Join(rs.opts.dir, fmt.Sprintf("filestore_%020d_%d.dat", time.Now().UnixNano(), CurrentFileVersion))
		err = os.Rename(out.Name(), newFileStoreName)
		if err != nil {
			panic(err)
		}

		rs.mx.Lock()
		delete(rs.memStores, req.idx)
		rs.fileStore = &fileStore{rs.t, rs.opts, newFileStoreName}
		rs.mx.Unlock()

		flushDuration := time.Now().Sub(start)
		rs.flushFinished <- flushDuration
		wasSorted := "not sorted"
		if req.sort {
			wasSorted = "sorted"
		}
		if fi != nil {
			rs.t.log.Debugf("Flushed to %v in %v, size %v. %v.", newFileStoreName, flushDuration, humanize.Bytes(uint64(fi.Size())), wasSorted)
		} else {
			rs.t.log.Debugf("Flushed to %v in %v. %v.", newFileStoreName, flushDuration, wasSorted)
		}
	}
}

func (rs *rowStore) removeOldFiles() {
	for {
		time.Sleep(1 * time.Minute)
		files, err := ioutil.ReadDir(rs.opts.dir)
		if err != nil {
			log.Errorf("Unable to list data files in %v: %v", rs.opts.dir, err)
		}
		now := time.Now()
		// Note - the list of files is sorted by name, which in our case is the
		// timestamp, so that means they're sorted chronologically. We don't want
		// to delete the last file in the list because that's the current one.
		for i := 0; i < len(files)-1; i++ {
			file := files[i]
			name := filepath.Join(rs.opts.dir, file.Name())
			// To be safe, we wait a little before deleting files
			if now.Sub(file.ModTime()) > 5*time.Minute {
				log.Debugf("Removing old file %v", name)
				err := os.Remove(name)
				if err != nil {
					rs.t.log.Errorf("Unable to delete old file store %v, still consuming disk space unnecessarily: %v", name, err)
				}
			}
		}
	}
}

// fileStore stores rows on disk, encoding them as:
//   rowLength|keylength|key|numcolumns|col1len|col2len|...|lastcollen|col1|col2|...|lastcol
//
// rowLength is 64 bits and includes itself
// keylength is 16 bits and does not include itself
// key can be up to 64KB
// numcolumns is 16 bits (i.e. 65,536 columns allowed)
// col*len is 64 bits
type fileStore struct {
	t        *table
	opts     *rowStoreOptions
	filename string
}

func (fs *fileStore) iterate(onRow func(bytemap.ByteMap, []encoding.Sequence), memStores []*bytetree.Tree, fields ...string) error {
	ctx := time.Now().UnixNano()

	if fs.t.log.IsTraceEnabled() {
		fs.t.log.Tracef("Iterating with %d memstores from file %v", len(memStores), fs.filename)
	}

	truncateBefore := fs.t.truncateBefore()
	var includeField func(int) bool
	if len(fields) == 0 {
		includeField = func(i int) bool {
			return true
		}
	} else {
		includedFields := make([]bool, 0, len(fs.t.Fields))
		for _, field := range fs.t.Fields {
			includeThisField := len(fields) == 0
			if !includeThisField {
				for _, fieldName := range fields {
					if fieldName == field.Name {
						includeThisField = true
						break
					}
				}
			}
			includedFields = append(includedFields, includeThisField)
		}
		includeField = func(i int) bool {
			return includedFields[i]
		}
	}

	file, err := os.OpenFile(fs.filename, os.O_RDONLY, 0)
	if !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("Unable to open file %v: %v", fs.filename, err)
		}
		r := snappy.NewReader(bufio.NewReaderSize(file, 65536))

		fileVersion := 0
		parts := strings.Split(filepath.Base(fs.filename), "_")
		if len(parts) == 3 {
			versionString := strings.Split(parts[2], ".")[0]
			var versionErr error
			fileVersion, err = strconv.Atoi(versionString)
			if versionErr != nil {
				panic(fmt.Errorf("Unable to determine file version for file %v: %v", fs.filename, versionErr))
			}
		}

		fileFields := fs.t.Fields

		if fileVersion >= FileVersion_2 {
			// File contains header with field info, use it
			headerLength := uint32(0)
			versionErr := binary.Read(r, encoding.Binary, &headerLength)
			if versionErr != nil {
				return fmt.Errorf("Unexpected error reading header length: %v", versionErr)
			}
			headerBytes := make([]byte, headerLength)
			_, err = io.ReadFull(r, headerBytes)
			if err != nil {
				return err
			}
			fieldStrings := strings.Split(string(headerBytes), ",")
			fileFields = make([]sql.Field, 0, len(fieldStrings))
			for _, fieldString := range fieldStrings {
				foundField := false
				for _, field := range fs.t.Fields {
					if fieldString == field.String() {
						fileFields = append(fileFields, field)
						foundField = true
						break
					}
				}
				if !foundField {
					panic(fmt.Errorf("Unable to find field for %v", fieldString))
				}
			}
		}

		reverseFileFieldIndexes := make([]int, 0, len(fields))
		for _, candidate := range fileFields {
			idx := -1
			for i, field := range fs.t.Fields {
				if includeField(i) && field.String() == candidate.String() {
					idx = i
				}
			}
			reverseFileFieldIndexes = append(reverseFileFieldIndexes, idx)
		}

		// Read from file
		for {
			rowLength := uint64(0)
			err := binary.Read(r, encoding.Binary, &rowLength)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("Unexpected error reading row length: %v", err)
			}

			row := make([]byte, rowLength)
			encoding.Binary.PutUint64(row, rowLength)
			row = row[encoding.Width64bits:]
			_, err = io.ReadFull(r, row)
			if err != nil {
				return fmt.Errorf("Unexpected error while reading row: %v", err)
			}

			keyLength, row := encoding.ReadInt16(row)
			key, row := encoding.ReadByteMap(row, keyLength)

			numColumns, row := encoding.ReadInt16(row)
			colLengths := make([]int, 0, numColumns)
			for i := 0; i < numColumns; i++ {
				var colLength int
				colLength, row = encoding.ReadInt64(row)
				colLengths = append(colLengths, int(colLength))
			}

			includesAtLeastOneColumn := false
			columns := make([]encoding.Sequence, len(fs.t.Fields))
			for i, colLength := range colLengths {
				var seq encoding.Sequence
				seq, row = encoding.ReadSequence(row, colLength)
				idx := reverseFileFieldIndexes[i]
				if idx > -1 {
					columns[idx] = seq
					if seq != nil {
						includesAtLeastOneColumn = true
					}
				}
				if fs.t.log.IsTraceEnabled() {
					fs.t.log.Tracef("File Read: %v", seq.String(fileFields[i].Expr))
				}
			}

			for _, ms := range memStores {
				columns2 := ms.Remove(ctx, key)

				// Merge memStore columns into fileStore columns
				for i, field := range fs.t.Fields {
					if !includeField(i) {
						continue
					}
					if i >= len(columns2) {
						continue
					}
					column2 := columns2[i]
					if column2 == nil {
						continue
					}
					includesAtLeastOneColumn = true
					column := columns[i]
					if column == nil {
						// Nothing to merge, just use column2
						columns[i] = column2
						continue
					}
					// merge
					columns[i] = column.Merge(column2, field.Expr, fs.t.Resolution, truncateBefore)
				}
			}

			if includesAtLeastOneColumn {
				onRow(key, columns)
			}
		}
	}

	// Read remaining stuff from mem stores
	for s, ms := range memStores {
		ms.Walk(ctx, func(key []byte, columns1 []encoding.Sequence) bool {
			columns := make([]encoding.Sequence, len(fs.t.Fields))
			for i, column := range columns1 {
				if includeField(i) {
					columns[i] = column
				}
			}
			for j := s + 1; j < len(memStores); j++ {
				ms2 := memStores[j]
				columns2 := ms2.Remove(ctx, key)
				for i, field := range fs.t.Fields {
					if !includeField(i) {
						continue
					}
					if i >= len(columns2) {
						continue
					}
					column2 := columns2[i]
					if column2 == nil {
						continue
					}
					column := columns[i]
					if column == nil {
						// Nothing to merge, just use column2
						columns[i] = column2
						continue
					}
					// merge
					columns[i] = column.Merge(column2, field.Expr, fs.t.Resolution, truncateBefore)
				}
			}
			onRow(bytemap.ByteMap(key), columns)

			return false
		})
	}

	return nil
}

type closerAdapter struct {
	w io.Writer
}

func (ca *closerAdapter) Write(b []byte) (int, error) {
	return ca.w.Write(b)
}

func (ca *closerAdapter) Close() error {
	return nil
}
