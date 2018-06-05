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
	"github.com/getlantern/errors"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/bytetree"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/golang/snappy"
	"github.com/oxtoacart/emsort"
)

const (
	// File format versions
	FileVersion_4      = 4
	CurrentFileVersion = FileVersion_4

	offsetFilename = "offset"
)

var (
	fieldsDelims = map[int]string{
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
	fields              core.Fields
	fieldUpdates        chan core.Fields
	opts                *rowStoreOptions
	memStore            *memstore
	fileStore           *fileStore
	inserts             chan *insert
	forceFlushes        chan bool
	forceFlushCompletes chan bool
	flushCount          int
	mx                  sync.RWMutex
}

type memstore struct {
	fields        core.Fields
	tree          *bytetree.Tree
	offset        wal.Offset
	offsetChanged bool
}

func (ms *memstore) copy() *memstore {
	return &memstore{
		fields:        ms.fields,
		tree:          ms.tree.Copy(),
		offset:        ms.offset,
		offsetChanged: ms.offsetChanged,
	}
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

			// Version is currently unused, just read it to advance through file
			versionFor(existingFileName)

			// Get WAL offset
			newWALOffset, opened, err := readWALOffset(existingFileName)
			if err != nil {
				if !opened {
					return nil, nil, err
				} else {
					log.Errorf("Unable to read offset from existing file %v, assuming corrupted and will remove: %v", existingFileName, err)
					rmErr := os.Remove(existingFileName)
					if rmErr != nil {
						return nil, nil, fmt.Errorf("Unable to remove corrupted file %v: %v", existingFileName, err)
					}
					continue
				}
			}

			if newWALOffset.After(walOffset) {
				walOffset = newWALOffset
			}
			t.log.Debugf("Initializing row store from %v", existingFileName)
			break
		}
	}

	fields := t.getFields()
	rs := &rowStore{
		opts:                opts,
		t:                   t,
		fields:              fields,
		fieldUpdates:        make(chan core.Fields),
		inserts:             make(chan *insert),
		forceFlushes:        make(chan bool),
		forceFlushCompletes: make(chan bool),
		fileStore: &fileStore{
			t:        t,
			fields:   fields,
			filename: existingFileName,
		},
	}
	rs.fileStore.rs = rs

	go rs.processInserts()
	go rs.removeOldFiles()

	return rs, walOffset, nil
}

func readWALOffset(filename string) (wal.Offset, bool, error) {
	opened := false
	file, err := os.Open(filename)
	if err != nil {
		return nil, opened, fmt.Errorf("Unable to open file %v: %v", filename, err)
	}
	defer file.Close()
	opened = true

	r := snappy.NewReader(file)

	// Read WAL along with preceeding header length
	walOffset := make(wal.Offset, wal.OffsetSize+4)
	_, err = io.ReadFull(r, walOffset)
	if err != nil {
		return nil, opened, err
	}

	// Drop header length
	return walOffset[4:], opened, nil
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

func (rs *rowStore) newMemStore() *memstore {
	fields := rs.fields
	tree := bytetree.New(fields.Exprs(), nil, rs.t.Resolution, 0, time.Time{}, time.Time{}, 0)
	return &memstore{fields: fields, tree: tree}
}

func (rs *rowStore) processInserts() {
	ms := rs.newMemStore()
	rs.mx.Lock()
	rs.memStore = ms
	rs.mx.Unlock()

	flushInterval := rs.opts.maxFlushLatency
	flushTimer := time.NewTimer(flushInterval)
	rs.t.log.Debugf("Will flush after %v", flushInterval)

	flush := func(allowSort bool) *memstore {
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
			return nil
		}
		if rs.t.log.IsTraceEnabled() {
			rs.t.log.Tracef("Requesting flush at memstore size: %v", humanize.Bytes(uint64(ms.tree.Bytes())))
		}
		newMS, flushDuration := rs.processFlush(ms, allowSort)
		ms = newMS
		flushInterval = flushDuration * 10
		if flushInterval > rs.opts.maxFlushLatency {
			flushInterval = rs.opts.maxFlushLatency
		} else if flushInterval < rs.opts.minFlushLatency {
			flushInterval = rs.opts.minFlushLatency
		}
		flushTimer.Reset(flushInterval)
		return newMS
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
		case fields := <-rs.fieldUpdates:
			rs.t.log.Debugf("Updating fields to %v", fields)
			// update fields immediately
			rs.fields = fields

			// force flush before processing any more inserts
			ms = flush(false)

			if ms == nil {
				// nothing flushed, create a new memstore to pick up new fields
				ms = rs.newMemStore()
				rs.mx.Lock()
				rs.memStore = ms
				rs.mx.Unlock()
			}
		}
	}
}

func (rs *rowStore) iterate(ctx context.Context, outFields core.Fields, includeMemStore bool, onValue func(bytemap.ByteMap, []encoding.Sequence) (more bool, err error)) (time.Time, error) {
	guard := core.Guard(ctx)

	rs.mx.RLock()
	fs := rs.fileStore
	var ms *memstore
	if includeMemStore {
		ms = rs.memStore.copy()
	}
	rs.mx.RUnlock()
	return fs.iterate(outFields, ms, false, false, func(key bytemap.ByteMap, columns []encoding.Sequence, raw []byte) (bool, error) {
		return guard.ProceedAfter(onValue(key, columns))
	})
}

func (rs *rowStore) processFlush(ms *memstore, allowSort bool) (*memstore, time.Duration) {
	shouldSort := allowSort && rs.t.shouldSort()
	willSort := "not sorted"
	if shouldSort {
	}

	if shouldSort {
		defer rs.t.stopSorting()
		willSort = "sorted"
	}

	rs.mx.RLock()
	fs := rs.fileStore
	rs.mx.RUnlock()
	// We allow raw most of the time for efficiency purposes, but every 10 flushes
	// we don't so that we have an opportunity to truncate old data.
	disallowRaw := rs.flushCount%10 == 9
	rs.flushCount++
	if disallowRaw {
		rs.t.log.Debug("Disallowing raw on flush to force truncation")
	}

	fs.t.log.Debugf("Starting flush, %v", willSort)
	start := time.Now()

	out, err := ioutil.TempFile("", "nextrowstore")
	if err != nil {
		panic(err)
	}
	defer out.Close()

	highWaterMark := fs.flush(out, rs.fields, nil, ms.offset, ms, shouldSort, disallowRaw)

	fi, err := out.Stat()
	if err != nil {
		fs.t.log.Errorf("Unable to stat output file to get size: %v", err)
	}
	// Note - we left-pad the unix nano value to the widest possible length to
	// ensure lexicographical sort matches time-based sort (e.g. on directory
	// listing).
	newFileStoreName := filepath.Join(rs.opts.dir, fmt.Sprintf("filestore_%020d_%d.dat", time.Now().UnixNano(), CurrentFileVersion))
	err = os.Rename(out.Name(), newFileStoreName)
	if err != nil {
		panic(err)
	}

	fs = &fileStore{rs.t, rs, rs.fields, newFileStoreName}
	ms = rs.newMemStore()
	rs.mx.Lock()
	rs.fileStore = fs
	rs.memStore = ms
	rs.mx.Unlock()

	flushDuration := time.Now().Sub(start)
	if fi != nil {
		rs.t.log.Debugf("Flushed to %v in %v, size %v. %v.", newFileStoreName, flushDuration, humanize.Bytes(uint64(fi.Size())), willSort)
	} else {
		rs.t.log.Debugf("Flushed to %v in %v. %v.", newFileStoreName, flushDuration, willSort)
	}

	rs.t.updateHighWaterMarkDisk(highWaterMark)
	return ms, flushDuration
}

func (fs *fileStore) flush(out *os.File, fields core.Fields, filter goexpr.Expr, offset wal.Offset, ms *memstore, shouldSort bool, disallowRaw bool) int64 {
	cout, err := fs.createOutWriter(out, fields, offset, shouldSort)
	if err != nil {
		panic(err)
	}

	highWaterMark := int64(0)
	truncateBefore := fs.t.truncateBefore()
	write := func(key bytemap.ByteMap, columns []encoding.Sequence, raw []byte) (bool, error) {
		nextHighWaterMark, err := fs.doWrite(cout, fields, filter, truncateBefore, shouldSort, key, columns, raw)
		if err != nil {
			panic(err)
		}
		if nextHighWaterMark > highWaterMark {
			highWaterMark = nextHighWaterMark
		}
		return true, nil
	}

	fs.iterate(fields, ms, !shouldSort, !disallowRaw, write)
	err = cout.Close()
	if err != nil {
		panic(err)
	}

	return highWaterMark
}

func (fs *fileStore) createOutWriter(out *os.File, fields core.Fields, offset wal.Offset, shouldSort bool) (io.WriteCloser, error) {
	sout := snappy.NewBufferedWriter(out)

	fieldStrings := make([]string, 0, len(fields))
	for _, field := range fields {
		fieldStrings = append(fieldStrings, field.String())
	}
	fieldsBytes := []byte(strings.Join(fieldStrings, fieldsDelims[CurrentFileVersion]))
	headerLength := uint32(len(offset) + len(fieldsBytes))
	err := binary.Write(sout, encoding.Binary, headerLength)
	if err != nil {
		return nil, errors.New("Unable to write header length: %v", err)
	}
	_, err = sout.Write(offset)
	if err != nil {
		return nil, errors.New("Unable to write header: %v", err)
	}
	_, err = sout.Write(fieldsBytes)
	if err != nil {
		return nil, errors.New("Unable to write header: %v", err)
	}

	if !shouldSort {
		return sout, nil
	}
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

	cout, sortErr := emsort.New(sout, chunk, less, int(fs.t.db.maxMemoryBytes())/10)
	if sortErr != nil {
		panic(sortErr)
	}

	return cout, nil
}

func (fs *fileStore) doWrite(cout io.WriteCloser, fields core.Fields, filter goexpr.Expr, truncateBefore time.Time, shouldSort bool, key bytemap.ByteMap, columns []encoding.Sequence, raw []byte) (int64, error) {
	highWaterMark := int64(0)

	if !shouldSort && raw != nil {
		// This is an optimization that allows us to skip other processing by just
		// passing through the raw data
		_, writeErr := cout.Write(raw)
		return highWaterMark, writeErr
	}

	if filter != nil && !filter.Eval(key).(bool) {
		// Didn't meet filter criteria, remove key
		return highWaterMark, nil
	}

	hasActiveSequence := false
	for i, seq := range columns {
		seq = seq.Truncate(fields[i].Expr.EncodedWidth(), fs.t.Resolution, truncateBefore, time.Time{})
		columns[i] = seq
		if seq != nil {
			hasActiveSequence = true
		}
	}

	if !hasActiveSequence {
		// all encoding.Sequences expired, remove key
		return highWaterMark, nil
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

	err := binary.Write(o, encoding.Binary, uint64(rowLength))
	if err != nil {
		return highWaterMark, errors.Wrap(err)
	}

	err = binary.Write(o, encoding.Binary, uint16(len(key)))
	if err != nil {
		return highWaterMark, errors.Wrap(err)
	}
	_, err = o.Write(key)
	if err != nil {
		return highWaterMark, errors.Wrap(err)
	}

	err = binary.Write(o, encoding.Binary, uint16(len(columns)))
	if err != nil {
		return highWaterMark, errors.Wrap(err)
	}
	for _, seq := range columns {
		err = binary.Write(o, encoding.Binary, uint64(len(seq)))
		if err != nil {
			return highWaterMark, errors.Wrap(err)
		}
	}
	for _, seq := range columns {
		_, err = o.Write(seq)
		if err != nil {
			return highWaterMark, errors.Wrap(err)
		}
	}

	if shouldSort {
		// flush buffer
		_b := buf.Bytes()
		_, writeErr := cout.Write(_b)
		if writeErr != nil {
			return highWaterMark, errors.Wrap(err)
		}
	}

	return highWaterMark, nil
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
			rs.t.db.waitForBackupToFinish()
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
	rs       *rowStore
	fields   core.Fields
	filename string
}

func (fs *fileStore) iterate(outFields []core.Field, ms *memstore, okayToReuseBuffer bool, rawOkay bool, onRow func(bytemap.ByteMap, []encoding.Sequence, []byte) (more bool, err error)) (time.Time, error) {
	ctx := time.Now().UnixNano()
	var highWaterMark time.Time

	if fs.t.log.IsTraceEnabled() {
		fs.t.log.Tracef("Iterating with memstore ? %v from file %v", ms != nil, fs.filename)
	}

	truncateBefore := fs.t.truncateBefore()
	if len(outFields) == 0 {
		// default outFields to in fields
		outFields = fs.fields
	}

	// this function will map fields from the memstore into the right positions on
	// the outbound row
	var memToOut func(out []encoding.Sequence, i int, seq encoding.Sequence) bool
	if ms != nil {
		memToOut = rowMerger(outFields, ms.fields, fs.t.Resolution, truncateBefore)
	}

	file, err := os.OpenFile(fs.filename, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		// no filestore available (yet), try reading the offset file
		o, err := ioutil.ReadFile(filepath.Join(fs.rs.opts.dir, offsetFilename))
		if err == nil && len(o) == wal.OffsetSize {
			highWaterMark = wal.Offset(o).TS()
			log.Debugf("Set highWaterMark from offset file: %v", highWaterMark)
		}
	} else {
		if err != nil {
			return highWaterMark, fmt.Errorf("Unable to open file %v: %v", fs.filename, err)
		}
		r := snappy.NewReader(file)

		fileVersion := versionFor(fs.filename)
		// File contains header with field info, use it
		headerLength := uint32(0)
		lengthErr := binary.Read(r, encoding.Binary, &headerLength)
		if lengthErr != nil {
			return highWaterMark, fmt.Errorf("Unexpected error reading header length: %v", lengthErr)
		}
		fieldsBytes := make([]byte, headerLength)
		_, err = io.ReadFull(r, fieldsBytes)
		if err != nil {
			return highWaterMark, err
		}
		highWaterMark = wal.Offset(fieldsBytes[:wal.OffsetSize]).TS()
		log.Debugf("Set highWaterMark from data file: %v", highWaterMark)
		fieldsBytes = fieldsBytes[wal.OffsetSize:]
		delim := fieldsDelims[fileVersion]
		fieldStrings := strings.Split(string(fieldsBytes), delim)
		fileFields := make(core.Fields, 0, len(fieldStrings))
		for _, fieldString := range fieldStrings {
			foundField := false
			for _, field := range fs.fields {
				if fieldString == field.String() {
					fileFields = append(fileFields, field)
					foundField = true
					break
				}
			}
			if !foundField {
				fileFields = append(fileFields, core.Field{})
			}
		}

		// raw is only okay if the file fields match the out fields
		rawOkay = rawOkay && fileFields.Equals(outFields)

		// this function will map fields from the file into the right positions on
		// the outbound row
		fileToOut := rowMapper(outFields, fileFields)

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
				return highWaterMark, fmt.Errorf("Unexpected error reading row length: %v", err)
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
				return highWaterMark, fmt.Errorf("Unexpected error while reading row: %v", err)
			}

			keyLength, row := encoding.ReadInt16(row)
			key, row := encoding.ReadByteMap(row, keyLength)

			var msColumns []encoding.Sequence
			if ms != nil {
				msColumns = ms.tree.Remove(ctx, key)
			}
			if msColumns == nil && rawOkay {
				// There's nothing to merge in, just pass through the raw data
				more, err := onRow(key, nil, raw)
				if !more || err != nil {
					return highWaterMark, err
				}
				continue
			}
			// At this point, we should never pass the raw data
			raw = nil

			numColumns, row := encoding.ReadInt16(row)
			colLengths := make([]int, 0, numColumns)
			for i := 0; i < numColumns; i++ {
				if len(row) < 8 {
					return highWaterMark, fmt.Errorf("Not enough data left to decode column length!")
				}
				var colLength int
				colLength, row = encoding.ReadInt64(row)
				colLengths = append(colLengths, int(colLength))
			}

			includesAtLeastOneColumn := false
			columns := make([]encoding.Sequence, len(outFields))
			for i, colLength := range colLengths {
				var seq encoding.Sequence
				if colLength > len(row) {
					return highWaterMark, fmt.Errorf("Not enough data left to decode column, wanted %d have %d", colLength, len(row))
				}
				seq, row = encoding.ReadSequence(row, colLength)
				if seq != nil && fileToOut(columns, i, seq) {
					includesAtLeastOneColumn = true
				}
				if fs.t.log.IsTraceEnabled() {
					fs.t.log.Tracef("File Read: %v", seq.String(fileFields[i].Expr, fs.t.Resolution))
				}
			}

			// Merge memStore columns into fileStore columns
			for i, msColumn := range msColumns {
				if memToOut(columns, i, msColumn) {
					includesAtLeastOneColumn = true
				}
			}

			var more bool
			if includesAtLeastOneColumn {
				more, err = onRow(key, columns, raw)
			}

			if !more || err != nil {
				return highWaterMark, err
			}
		}
	}

	// Read remaining stuff from memstore
	if ms != nil {
		msts := ms.offset.TS()
		if msts.After(highWaterMark) {
			highWaterMark = msts
			log.Debugf("Set highWaterMark from memstore: %v", highWaterMark)
		}
		ms.tree.Walk(ctx, func(key []byte, msColumns []encoding.Sequence) (bool, bool, error) {
			columns := make([]encoding.Sequence, len(outFields))
			for i, msColumn := range msColumns {
				memToOut(columns, i, msColumn)
			}
			more, err := onRow(bytemap.ByteMap(key), columns, nil)
			return more, false, err
		})
	}

	return highWaterMark, nil
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

func rowMapper(outFields core.Fields, inFields core.Fields) func(out []encoding.Sequence, i int, seq encoding.Sequence) bool {
	outIdxs := outIdxsFor(outFields, inFields)

	return func(out []encoding.Sequence, i int, seq encoding.Sequence) bool {
		o := outIdxs[i]
		if o >= 0 {
			out[o] = seq
			return true
		}
		return false
	}
}

func rowMerger(outFields core.Fields, inFields core.Fields, resolution time.Duration, truncateBefore time.Time) func(out []encoding.Sequence, i int, seq encoding.Sequence) bool {
	outIdxs := outIdxsFor(outFields, inFields)

	return func(out []encoding.Sequence, i int, seq encoding.Sequence) bool {
		if i >= len(outIdxs) {
			return false
		}

		o := outIdxs[i]
		if o >= 0 {
			out[o] = out[o].Merge(seq, outFields[o].Expr, resolution, truncateBefore)
			return true
		}
		return false
	}
}

func outIdxsFor(outFields core.Fields, inFields core.Fields) []int {
	outIdxs := make([]int, 0, len(inFields))
	for _, inField := range inFields {
		o := -1
		for _o, outField := range outFields {
			if inField.Equals(outField) {
				o = _o
				break
			}
		}
		outIdxs = append(outIdxs, o)
	}

	return outIdxs
}
