package zenodb

import (
	"bytes"
	"context"
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
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/bytetree"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"github.com/golang/snappy"
	"github.com/oxtoacart/emsort"
)

const (
	// File format versions
	FileVersion_2      = 2
	FileVersion_3      = 3
	FileVersion_4      = 4
	CurrentFileVersion = FileVersion_4

	offsetFilename = "offset"
)

var (
	fieldsDelims = map[int]string{
		FileVersion_2: ",",
		FileVersion_3: "|",
		FileVersion_4: "|",
	}
)

type rowStoreOptions struct {
	dir             string
	minFlushLatency time.Duration
	maxFlushLatency time.Duration
}

type insert struct {
	key      bytemap.ByteMap
	vals     encoding.TSParams
	metadata bytemap.ByteMap
	offset   wal.Offset
}

type rowStore struct {
	t                   *table
	opts                *rowStoreOptions
	memStore            *memstore
	fileStore           *fileStore
	inserts             chan *insert
	forceFlushes        chan bool
	forceFlushCompletes chan bool
	mx                  sync.RWMutex
}

type memstore struct {
	tree          *bytetree.Tree
	offset        wal.Offset
	offsetChanged bool
}

func (t *table) openRowStore(opts *rowStoreOptions) (*rowStore, wal.Offset, error) {
	err := os.MkdirAll(opts.dir, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, nil, fmt.Errorf("Unable to create folder for row store: %v", err)
	}

	existingFileName := ""
	files, err := ioutil.ReadDir(opts.dir)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to read contents of directory: %v", err)
	}
	var walOffset wal.Offset
	if len(files) > 0 {
		// files are sorted by name, in our case timestamp, so the last file in the
		// list is the most recent. That's the one that we want.
		for i := len(files) - 1; i >= 0; i-- {
			filename := files[i].Name()
			existingFileName = filepath.Join(opts.dir, files[i].Name())
			if filename == offsetFilename {
				// This is an offset file, just read the offset
				o, err := ioutil.ReadFile(existingFileName)
				if err != nil {
					t.log.Errorf("Unable to read offset: %v", err)
				} else if len(o) != wal.OffsetSize {
					t.log.Errorf("Invalid offset found in offset file")
				} else {
					walOffset = wal.Offset(o)
				}
				continue
			}

			fileVersion := versionFor(existingFileName)
			if fileVersion >= FileVersion_4 {
				// Get WAL offset
				file, err := os.Open(existingFileName)
				if err != nil {
					return nil, nil, fmt.Errorf("Unable to open existing file %v: %v", existingFileName, err)
				}
				defer file.Close()
				r := snappy.NewReader(file)
				// Skip header length
				newWALOffset := make(wal.Offset, wal.OffsetSize+4)
				_, err = io.ReadFull(r, newWALOffset)
				if err != nil {
					log.Errorf("Unable to read offset from existing file %v, assuming corrupted and will remove: %v", existingFileName, err)
					rmErr := os.Remove(existingFileName)
					if rmErr != nil {
						return nil, nil, fmt.Errorf("Unable to remove corrupted file %v: %v", existingFileName, err)
					}
					continue
				}
				// Skip header length
				newWALOffset = newWALOffset[4:]
				if newWALOffset.After(walOffset) {
					walOffset = newWALOffset
				}
			}
			t.log.Debugf("Initializing row store from %v", existingFileName)
			break
		}
	}

	rs := &rowStore{
		opts:                opts,
		t:                   t,
		inserts:             make(chan *insert),
		forceFlushes:        make(chan bool),
		forceFlushCompletes: make(chan bool),
		fileStore: &fileStore{
			t:        t,
			opts:     opts,
			filename: existingFileName,
		},
	}

	go rs.processInserts()
	go rs.removeOldFiles()

	return rs, walOffset, nil
}

func (rs *rowStore) memStoreSize() int {
	size := 0
	rs.mx.RLock()
	if rs.memStore != nil {
		size = rs.memStore.tree.Bytes()
	}
	rs.mx.RUnlock()
	return size
}

func (rs *rowStore) insert(insert *insert) {
	rs.inserts <- insert
}

func (rs *rowStore) forceFlush() {
	rs.forceFlushes <- true
	<-rs.forceFlushCompletes
}

func (t *table) newByteTree() *bytetree.Tree {
	exprs := make([]expr.Expr, 0, len(t.Fields))
	for _, field := range t.Fields {
		exprs = append(exprs, field.Expr)
	}
	return bytetree.New(exprs, nil, t.Resolution, 0, time.Time{}, time.Time{}, 0)
}

func (rs *rowStore) processInserts() {
	ms := &memstore{tree: rs.t.newByteTree()}
	rs.mx.Lock()
	rs.memStore = ms
	rs.mx.Unlock()

	flushInterval := rs.opts.maxFlushLatency
	flushTimer := time.NewTimer(flushInterval)
	rs.t.log.Debugf("Will flush after %v", flushInterval)

	flush := func(allowSort bool) {
		if ms.tree.Length() == 0 {
			rs.t.log.Trace("No data to flush")

			if ms.offsetChanged {
				rs.t.log.Debug("No new data, but we've advanced through the WAL, record the change")
				err := rs.writeOffset(ms.offset)
				if err != nil {
					rs.t.log.Errorf("Unable to write updated offset: %v", err)
				}
				ms.offsetChanged = false
			}

			// Immediately reset flushTimer
			flushTimer.Reset(flushInterval)
			return
		}
		if rs.t.log.IsTraceEnabled() {
			rs.t.log.Tracef("Requesting flush at memstore size: %v", humanize.Bytes(uint64(ms.tree.Bytes())))
		}
		flushDuration := rs.processFlush(ms, allowSort)
		ms = &memstore{tree: rs.t.newByteTree()}
		rs.mx.Lock()
		rs.memStore = ms
		rs.mx.Unlock()
		flushInterval = flushDuration * 10
		if flushInterval > rs.opts.maxFlushLatency {
			flushInterval = rs.opts.maxFlushLatency
		} else if flushInterval < rs.opts.minFlushLatency {
			flushInterval = rs.opts.minFlushLatency
		}
		flushTimer.Reset(flushInterval)
	}

	for {
		select {
		case insert := <-rs.inserts:
			rs.mx.Lock()
			ms.offset = insert.offset
			ms.offsetChanged = true
			if insert.key != nil {
				ms.tree.Update(insert.key, nil, insert.vals, insert.metadata)
				rs.t.updateHighWaterMarkMemory(insert.vals.TimeInt())
			}
			rs.mx.Unlock()
		case <-flushTimer.C:
			rs.t.log.Trace("Requesting flush due to flush interval")
			flush(false)
		case <-rs.forceFlushes:
			rs.t.log.Debug("Forcing flush")
			flush(true)
			rs.forceFlushCompletes <- true
		}
	}
}

func (rs *rowStore) iterate(ctx context.Context, includedFields []string, includeMemStore bool, onValue func(bytemap.ByteMap, []encoding.Sequence) (more bool, err error)) error {
	guard := core.Guard(ctx)

	rs.mx.RLock()
	fs := rs.fileStore
	var tree *bytetree.Tree
	if includeMemStore {
		tree = rs.memStore.tree.Copy()
	}
	rs.mx.RUnlock()
	return fs.iterate(func(key bytemap.ByteMap, columns []encoding.Sequence, raw []byte) (bool, error) {
		return guard.ProceedAfter(onValue(key, columns))
	}, false, false, tree, includedFields...)
}

func (rs *rowStore) processFlush(ms *memstore, allowSort bool) time.Duration {
	shouldSort := allowSort && rs.t.shouldSort()
	willSort := "not sorted"
	if shouldSort {
		defer rs.t.stopSorting()
		willSort = "sorted"
	}

	rs.t.log.Debugf("Starting flush, %v", willSort)
	start := time.Now()
	out, err := ioutil.TempFile("", "nextrowstore")
	if err != nil {
		panic(err)
	}
	defer out.Close()
	sout := snappy.NewBufferedWriter(out)

	fieldStrings := make([]string, 0, len(rs.t.Fields))
	for _, field := range rs.t.Fields {
		fieldStrings = append(fieldStrings, field.String())
	}
	fieldsBytes := []byte(strings.Join(fieldStrings, fieldsDelims[CurrentFileVersion]))
	headerLength := uint32(len(ms.offset) + len(fieldsBytes))
	err = binary.Write(sout, encoding.Binary, headerLength)
	if err != nil {
		panic(fmt.Errorf("Unable to write header length: %v", err))
	}
	_, err = sout.Write(ms.offset)
	if err != nil {
		panic(fmt.Errorf("Unable to write header: %v", err))
	}
	_, err = sout.Write(fieldsBytes)
	if err != nil {
		panic(fmt.Errorf("Unable to write header: %v", err))
	}

	var cout io.WriteCloser
	if !shouldSort {
		cout = sout
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
		cout, sortErr = emsort.New(sout, chunk, less, int(rs.t.db.maxMemoryBytes())/10)
		if sortErr != nil {
			panic(sortErr)
		}
	}

	highWaterMark := int64(0)
	truncateBefore := rs.t.truncateBefore()
	write := func(key bytemap.ByteMap, columns []encoding.Sequence, raw []byte) (bool, error) {
		if !shouldSort && raw != nil {
			// This is an optimization that allows us to skip other processing by just
			// passing through the raw data
			_, writeErr := cout.Write(raw)
			return true, writeErr
		}

		hasActiveSequence := false
		for i, seq := range columns {
			seq = seq.Truncate(rs.t.Fields[i].Expr.EncodedWidth(), rs.t.Resolution, truncateBefore, time.Time{})
			columns[i] = seq
			if seq != nil {
				hasActiveSequence = true
			}
		}

		if !hasActiveSequence {
			// all encoding.Sequences expired, remove key
			return true, nil
		}

		rowLength := encoding.Width64bits + encoding.Width16bits + len(key) + encoding.Width16bits
		for _, seq := range columns {
			rowLength += encoding.Width64bits + len(seq)
			ts := seq.UntilInt()
			if ts > highWaterMark {
				highWaterMark = ts
			}
		}

		var o io.Writer = cout
		var buf *bytes.Buffer
		if shouldSort {
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

		if shouldSort {
			// flush buffer
			_b := buf.Bytes()
			_, writeErr := cout.Write(_b)
			if writeErr != nil {
				panic(writeErr)
			}
		}

		return true, nil
	}
	rs.mx.RLock()
	fs := rs.fileStore
	rs.mx.RUnlock()
	fs.iterate(write, !shouldSort, true, ms.tree)
	err = cout.Close()
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
	rs.fileStore = &fileStore{rs.t, rs.opts, newFileStoreName}
	rs.mx.Unlock()

	flushDuration := time.Now().Sub(start)
	if fi != nil {
		rs.t.log.Debugf("Flushed to %v in %v, size %v. %v.", newFileStoreName, flushDuration, humanize.Bytes(uint64(fi.Size())), willSort)
	} else {
		rs.t.log.Debugf("Flushed to %v in %v. %v.", newFileStoreName, flushDuration, willSort)
	}

	rs.t.updateHighWaterMarkDisk(highWaterMark)
	return flushDuration
}

func (rs *rowStore) writeOffset(offset wal.Offset) error {
	out, err := ioutil.TempFile("", "nextoffset")
	if err != nil {
		panic(err)
	}
	defer out.Close()

	_, err = out.Write(offset)
	if err != nil {
		return fmt.Errorf("Unable to write next offset: %v", err)
	}

	err = out.Close()
	if err != nil {
		return fmt.Errorf("Unable to close offset file: %v", err)
	}

	return os.Rename(out.Name(), filepath.Join(rs.opts.dir, offsetFilename))
}

func (rs *rowStore) removeOldFiles() {
	for {
		time.Sleep(10 * time.Second)
		files, err := ioutil.ReadDir(rs.opts.dir)
		if err != nil {
			log.Errorf("Unable to list data files in %v: %v", rs.opts.dir, err)
		}
		// Note - the list of files is sorted by name, which in our case is the
		// timestamp, so that means they're sorted chronologically. We don't want
		// to delete the last file in the list because that's the current one.
		foundLatest := false
		for i := len(files) - 1; i >= 0; i-- {
			filename := files[i].Name()
			if filename == offsetFilename {
				// Ignore offset file
				continue
			}
			if !foundLatest {
				foundLatest = true
				continue
			}
			// Okay to delete now
			name := filepath.Join(rs.opts.dir, filename)
			rs.t.log.Debugf("Removing old file %v", name)
			err := os.Remove(name)
			if err != nil {
				rs.t.log.Errorf("Unable to delete old file store %v, still consuming disk space unnecessarily: %v", name, err)
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

func (fs *fileStore) iterate(onRow func(bytemap.ByteMap, []encoding.Sequence, []byte) (more bool, err error), okayToReuseBuffer bool, rawOkay bool, tree *bytetree.Tree, includedFields ...string) error {
	ctx := time.Now().UnixNano()

	if fs.t.log.IsTraceEnabled() {
		fs.t.log.Tracef("Iterating with memstore ? %v from file %v", tree != nil, fs.filename)
	}

	truncateBefore := fs.t.truncateBefore()
	var includeField func(int) bool
	if len(includedFields) == 0 {
		includeField = func(i int) bool {
			return true
		}
	} else {
		includedFieldIdxs := make([]bool, 0, len(fs.t.Fields))
		for _, field := range fs.t.Fields {
			includeThisField := len(includedFields) == 0
			if !includeThisField {
				for _, fieldName := range includedFields {
					if fieldName == field.Name {
						includeThisField = true
						break
					}
				}
			}
			includedFieldIdxs = append(includedFieldIdxs, includeThisField)
		}
		includeField = func(i int) bool {
			return includedFieldIdxs[i]
		}
	}

	file, err := os.OpenFile(fs.filename, os.O_RDONLY, 0)
	if !os.IsNotExist(err) {
		if err != nil {
			return fmt.Errorf("Unable to open file %v: %v", fs.filename, err)
		}
		r := snappy.NewReader(file)

		fileVersion := versionFor(fs.filename)
		fileFields := fs.t.Fields
		if fileVersion >= FileVersion_2 {
			// File contains header with field info, use it
			headerLength := uint32(0)
			lengthErr := binary.Read(r, encoding.Binary, &headerLength)
			if lengthErr != nil {
				return fmt.Errorf("Unexpected error reading header length: %v", lengthErr)
			}
			fieldsBytes := make([]byte, headerLength)
			_, err = io.ReadFull(r, fieldsBytes)
			if err != nil {
				return err
			}
			if fileVersion >= FileVersion_4 {
				// Strip offset
				fieldsBytes = fieldsBytes[wal.OffsetSize:]
			}
			delim := fieldsDelims[fileVersion]
			fieldStrings := strings.Split(string(fieldsBytes), delim)
			fileFields = make([]core.Field, 0, len(fieldStrings))
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
					fs.t.log.Debugf("Unable to find field %v on table %v", fieldString, fs.t.Name)
					fileFields = append(fileFields, core.Field{})
					// It's not okay to use raw rows since field definitions have changed
					rawOkay = false
				}
			}
			if len(fieldStrings) != len(fs.t.Fields) {
				fs.t.log.Debug("File store has different set of fields than table, disabling raw passthrough")
				rawOkay = false
			}
		}

		reverseFileFieldIndexes := make([]int, 0, len(includedFields))
		for _, candidate := range fileFields {
			idx := -1
			if candidate.Expr != nil {
				for i, field := range fs.t.Fields {
					if includeField(i) && field.String() == candidate.String() {
						idx = i
					}
				}
			}
			reverseFileFieldIndexes = append(reverseFileFieldIndexes, idx)
		}

		var rowBuffer []byte
		var row []byte

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

			useBuffer := okayToReuseBuffer && int(rowLength) <= cap(rowBuffer)
			if useBuffer {
				// Reslice
				row = rowBuffer[:rowLength]
			} else {
				row = make([]byte, rowLength)
			}
			rowBuffer = row
			raw := row
			encoding.Binary.PutUint64(row, rowLength)
			row = row[encoding.Width64bits:]
			_, err = io.ReadFull(r, row)
			if err != nil {
				return fmt.Errorf("Unexpected error while reading row: %v", err)
			}

			keyLength, row := encoding.ReadInt16(row)
			key, row := encoding.ReadByteMap(row, keyLength)

			var columns2 []encoding.Sequence
			if tree != nil {
				columns2 = tree.Remove(ctx, key)
			}
			if columns2 == nil && rawOkay {
				// There's nothing to merge in, just pass through the raw data
				more, err := onRow(key, nil, raw)
				if !more || err != nil {
					return err
				}
				continue
			}

			numColumns, row := encoding.ReadInt16(row)
			colLengths := make([]int, 0, numColumns)
			for i := 0; i < numColumns; i++ {
				if len(row) < 8 {
					return fmt.Errorf("Not enough data left to decode column length!")
				}
				var colLength int
				colLength, row = encoding.ReadInt64(row)
				colLengths = append(colLengths, int(colLength))
			}

			includesAtLeastOneColumn := false
			columns := make([]encoding.Sequence, len(fs.t.Fields))
			for i, colLength := range colLengths {
				var seq encoding.Sequence
				if colLength > len(row) {
					return fmt.Errorf("Not enough data left to decode column, wanted %d have %d", colLength, len(row))
				}
				seq, row = encoding.ReadSequence(row, colLength)
				idx := reverseFileFieldIndexes[i]
				if idx > -1 {
					columns[idx] = seq
					if seq != nil {
						includesAtLeastOneColumn = true
					}
				}
				if fs.t.log.IsTraceEnabled() {
					fs.t.log.Tracef("File Read: %v", seq.String(fileFields[i].Expr, fs.t.Resolution))
				}
			}

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
				// Clear raw to prevent recipient from using passthrough data
				raw = nil
				column := columns[i]
				if column == nil {
					// Nothing to merge, just use column2
					columns[i] = column2
					continue
				}
				// merge
				columns[i] = column.Merge(column2, field.Expr, fs.t.Resolution, truncateBefore)
			}

			var more bool
			if includesAtLeastOneColumn {
				more, err = onRow(key, columns, raw)
			}

			if !more || err != nil {
				return err
			}
		}
	}

	// Read remaining stuff from memstore
	if tree != nil {
		tree.Walk(ctx, func(key []byte, columns1 []encoding.Sequence) (bool, bool, error) {
			columns := make([]encoding.Sequence, len(fs.t.Fields))
			for i, column := range columns1 {
				if includeField(i) {
					columns[i] = column
				}
			}
			more, err := onRow(bytemap.ByteMap(key), columns, nil)
			return more, false, err
		})
	}

	return nil
}

func versionFor(filename string) int {
	fileVersion := 0
	parts := strings.Split(filepath.Base(filename), "_")
	if len(parts) == 3 {
		versionString := strings.Split(parts[2], ".")[0]
		var versionErr error
		fileVersion, versionErr = strconv.Atoi(versionString)
		if versionErr != nil {
			panic(fmt.Errorf("Unable to determine file version for file %v: %v", filename, versionErr))
		}
	}
	return fileVersion
}
