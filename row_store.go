package zenodb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/oxtoacart/emsort"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/bytetree"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
)

const (
	// File format versions
	FileVersion_4      = 4
	FileVersion_5      = 5
	CurrentFileVersion = FileVersion_5

	offsetFilename = "offset"
)

var (
	fieldsDelims = map[int]string{
		FileVersion_4: "|",
		FileVersion_5: "|",
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
	source   int
}

type rowStore struct {
	t                    *table
	fields               core.Fields
	fieldUpdates         chan core.Fields
	opts                 *rowStoreOptions
	memStore             *memstore
	fileStore            *fileStore
	inserts              chan *insert
	forceFlushes         chan bool
	forceFlushCompletes  chan bool
	flushCount           int
	iterationsInProgress map[string]int
	mx                   sync.RWMutex
}

type memstore struct {
	fields          core.Fields
	tree            *bytetree.Tree
	offsetsBySource common.OffsetsBySource
	offsetChanged   bool
}

func (ms *memstore) copy() *memstore {
	copyOfOffsets := make(common.OffsetsBySource)
	for source, offset := range ms.offsetsBySource {
		copyOfOffsets[source] = offset
	}
	return &memstore{
		fields:          ms.fields,
		tree:            ms.tree.Copy(),
		offsetsBySource: copyOfOffsets,
		offsetChanged:   ms.offsetChanged,
	}
}

func (t *table) openRowStore(opts *rowStoreOptions) (*rowStore, common.OffsetsBySource, error) {
	err := os.MkdirAll(opts.dir, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, nil, errors.New("Unable to create folder for row store: %v", err)
	}

	existingFileName := ""
	files, err := listRegularFiles(opts.dir)
	if err != nil {
		return nil, nil, errors.New("Unable to read contents of directory: %v", err)
	}
	offsetsBySource := make(common.OffsetsBySource)
	if len(files) > 0 {
		for _, file := range files {
			t.log.Debug(file.Name())
		}
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
				} else if len(o) < wal.OffsetSize {
					t.log.Errorf("Offset file contents of wrong length: %v %d", existingFileName, len(o))
				} else {
					fileVersion := FileVersion_4
					if len(o) > wal.OffsetSize {
						// Before Version 5, we only stored a single offset. Since we have more than that,
						// assume that this is at least Version 5.
						fileVersion = FileVersion_5
					}
					offsetsBySource, _ = t.readOffsets(fileVersion, o)
					t.log.Debugf("Read highWaterMarks from offset file: %v", offsetsBySource.TSString())
				}
				// clear existingFileName so we don't use it for filestore
				existingFileName = ""
				continue
			}

			// Get WAL offset
			newOffsetsBySource, opened, err := t.readWALOffsets(existingFileName)
			if err != nil {
				if !opened {
					return nil, nil, err
				} else {
					t.log.Errorf("Unable to read offset from existing file %v, assuming corrupted and will remove: %v", existingFileName, err)
					rmErr := os.Remove(existingFileName)
					if rmErr != nil {
						return nil, nil, errors.New("Unable to remove corrupted file %v: %v", existingFileName, err)
					}
					continue
				}
			}

			offsetsBySource = newOffsetsBySource.Advance(offsetsBySource)
			t.log.Debugf("Initializing row store from %v", existingFileName)
			break
		}
	}

	fields := t.getFields()
	rs := &rowStore{
		opts:                 opts,
		t:                    t,
		fields:               fields,
		fieldUpdates:         make(chan core.Fields),
		inserts:              make(chan *insert),
		forceFlushes:         make(chan bool),
		forceFlushCompletes:  make(chan bool),
		iterationsInProgress: make(map[string]int),
		fileStore: &fileStore{
			t:        t,
			fields:   fields,
			filename: existingFileName,
		},
	}
	rs.fileStore.rs = rs

	t.db.Go(func(stop <-chan interface{}) {
		rs.processInserts(offsetsBySource, stop)
	})
	t.db.Go(rs.removeOldFiles)

	return rs, offsetsBySource, nil
}

func (t *table) readWALOffsets(filename string) (common.OffsetsBySource, bool, error) {
	opened := false
	var offsetsBySource common.OffsetsBySource

	t.log.Debugf("Reading WAL offsets from %v", filename)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return offsetsBySource, opened, errors.New("Unable to open file %v: %v", filename, err)
	}
	defer file.Close()
	opened = true

	fileVersion := t.versionFor(filename)

	r := snappy.NewReader(file)

	headerLength := uint32(0)
	lengthErr := binary.Read(r, encoding.Binary, &headerLength)
	if lengthErr != nil {
		return offsetsBySource, opened, errors.New("Unexpected error reading header length from %v: %v", filename, lengthErr)
	}
	fieldsBytes := make([]byte, headerLength)
	_, readErr := io.ReadFull(r, fieldsBytes)
	if readErr != nil {
		return offsetsBySource, opened, errors.New("Unable to read fields from %v: %v", filename, readErr)
	}
	offsetsBySource, fieldsBytes = t.readOffsets(fileVersion, fieldsBytes)
	return offsetsBySource, opened, nil
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

func (rs *rowStore) newMemStore(offsetsBySource common.OffsetsBySource) *memstore {
	fields := rs.fields
	tree := bytetree.New(fields.Exprs(), nil, rs.t.Resolution, 0, time.Time{}, time.Time{}, 0)
	return &memstore{fields: fields, tree: tree, offsetsBySource: offsetsBySource}
}

func (rs *rowStore) processInserts(offsetsBySource common.OffsetsBySource, stop <-chan interface{}) {
	ms := rs.newMemStore(offsetsBySource)
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
				err := rs.writeOffsets(ms.offsetsBySource)
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
			ms.offsetsBySource[insert.source] = insert.offset
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
		case <-stop:
			rs.t.log.Debug("Forcing flush due to database stopped")
			flush(true)
			rs.t.log.Debug("Done forcing flush due to database stopped")
			return
		case fields := <-rs.fieldUpdates:
			rs.t.log.Debugf("Updating fields to %v", fields)
			// update fields immediately
			rs.fields = fields

			// force flush before processing any more inserts
			offsetsBySource = ms.offsetsBySource
			ms = flush(false)

			if ms == nil {
				// nothing flushed, create a new memstore to pick up new fields
				ms = rs.newMemStore(offsetsBySource)
				rs.mx.Lock()
				rs.memStore = ms
				rs.mx.Unlock()
			}
		}
	}
}

func (rs *rowStore) iterate(ctx context.Context, outFields core.Fields, includeMemStore bool, onValue func(bytemap.ByteMap, []encoding.Sequence) (more bool, err error)) (common.OffsetsBySource, error) {
	guard := core.Guard(ctx)

	rs.mx.RLock()
	fs := rs.fileStore
	var ms *memstore
	if includeMemStore {
		ms = rs.memStore.copy()
	}
	rs.mx.RUnlock()
	rs.mx.Lock()
	rs.iterationsInProgress[fs.filename]++
	rs.mx.Unlock()
	defer func() {
		rs.mx.Lock()
		rs.iterationsInProgress[fs.filename]--
		rs.mx.Unlock()
	}()
	return fs.iterate(outFields, ms, false, false, func(key bytemap.ByteMap, columns []encoding.Sequence, raw []byte) (bool, error) {
		return guard.ProceedAfter(onValue(key, columns))
	})
}

func (rs *rowStore) processFlush(ms *memstore, allowSort bool) (*memstore, time.Duration) {
	attempts := 3
	for i := 0; i < attempts; i++ {
		// Try a few times just in case we encounter a random error reading the file
		last := i == attempts-1
		result, duration := rs.doProcessFlush(ms, allowSort, !last)
		if result != nil {
			return result, duration
		}
	}
	rs.t.db.Panic("processFlush loop terminated without result, should never happen")
	return nil, 0
}

func (rs *rowStore) doProcessFlush(ms *memstore, allowSort, allowFailure bool) (*memstore, time.Duration) {
	shouldSort := allowSort && rs.t.shouldSort()
	willSort := "not sorted"
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
		rs.t.db.Panic(err)
	}
	defer out.Close()

	highWaterMark, rowCount, flushErr := fs.flush(out, rs.fields, nil, ms.offsetsBySource, ms, shouldSort, disallowRaw)
	if flushErr != nil {
		shasum, err := calcShaSum(fs.filename)
		if err != nil {
			rs.t.log.Errorf("Unable to calculate sha256 sum for %v: %v", fs.filename, err)
		} else {
			rs.t.log.Debugf("sha256sum for %v was %v after failing to iterate", fs.filename, shasum)
		}
		if allowFailure {
			rs.t.log.Errorf("Unable to flush using %v, failed after reading %d rows, will try again: %v", fs.filename, rowCount, flushErr)
			return nil, 0
		}
		rs.t.log.Errorf("Unable to flush using %v, failed after reading %d rows, marking file as corrupted and panicking: %v", fs.filename, rowCount, flushErr)
		fs.markCorrupted()
		rs.t.db.Panic(flushErr)
	}

	if syncErr := out.Sync(); syncErr != nil {
		rs.t.db.Panic(syncErr)
	}
	fi, err := out.Stat()
	if err != nil {
		fs.t.log.Errorf("Unable to stat output file to get size: %v", err)
	}
	if closeErr := out.Close(); closeErr != nil {
		rs.t.db.Panic(closeErr)
	}

	// Note - we left-pad the unix nano value to the widest possible length to
	// ensure lexicographical sort matches time-based sort (e.g. on directory
	// listing).
	newFileStoreName := filepath.Join(rs.opts.dir, fmt.Sprintf("filestore_%020d_%d.dat", time.Now().UnixNano(), CurrentFileVersion))
	if renameErr := os.Rename(out.Name(), newFileStoreName); renameErr != nil {
		rs.t.db.Panic(renameErr)
	}
	defer func() {
		shasum, err := calcShaSum(newFileStoreName)
		if err != nil {
			rs.t.log.Errorf("Unable to calculate sha256 sum for %v: %v", newFileStoreName, err)
		} else {
			rs.t.log.Debugf("sha256sum for %v was %v immediately after writing", newFileStoreName, shasum)
		}
	}()

	fs = &fileStore{rs.t, rs, rs.fields, newFileStoreName}
	ms = rs.newMemStore(ms.offsetsBySource)
	rs.mx.Lock()
	rs.fileStore = fs
	rs.memStore = ms
	rs.mx.Unlock()

	flushDuration := time.Now().Sub(start)
	if fi != nil {
		rs.t.log.Debugf("Flushed %d rows to %v in %v, compressed size on disk %d. %v.", rowCount, newFileStoreName, flushDuration, fi.Size(), willSort)
	} else {
		rs.t.log.Debugf("Flushed %d rows to %v in %v. %v.", rowCount, newFileStoreName, flushDuration, willSort)
	}

	rs.t.updateHighWaterMarkDisk(highWaterMark)
	return ms, flushDuration
}

func (fs *fileStore) flush(out *os.File, fields core.Fields, filter goexpr.Expr, offsetsBySource common.OffsetsBySource, ms *memstore, shouldSort bool, disallowRaw bool) (int64, int, error) {
	cout, err := fs.createOutWriter(out, fields, offsetsBySource, shouldSort)
	if err != nil {
		fs.t.db.Panic(fmt.Errorf("Unable to create out writer: %v", err))
	}

	highWaterMark := int64(0)
	truncateBefore := fs.t.truncateBefore()
	rowCount := 0
	write := func(key bytemap.ByteMap, columns []encoding.Sequence, raw []byte) (bool, error) {
		nextHighWaterMark, err := fs.doWrite(cout, fields, filter, truncateBefore, shouldSort, key, columns, raw)
		if err != nil {
			fs.t.db.Panic(fmt.Errorf("Unable to write row out: %v", err))
		}
		if nextHighWaterMark > highWaterMark {
			highWaterMark = nextHighWaterMark
		}
		rowCount++
		return true, nil
	}

	_, err = fs.iterate(fields, ms, !shouldSort, !disallowRaw, write)
	if err != nil {
		// this is the only case in which we return an error to signify that we can self-heal by deleting this filestore
		return highWaterMark, rowCount, err
	}

	// manually flush to the underlying snappy writer, since snappy's own Close() function doesn't check the return value of flush
	f, ok := cout.(flushable)
	if ok {
		err = f.Flush()
		if err != nil {
			cout.Close()
			fs.t.db.Panic(fmt.Errorf("Unable to flush flushable writer: %v", err))
		}
	}

	err = cout.Close()
	if err != nil {
		fs.t.db.Panic(fmt.Errorf("Unable to close out writer: %v", err))
	}

	return highWaterMark, rowCount, nil
}

type flushable interface {
	Flush() error
}

func (fs *fileStore) createOutWriter(out *os.File, fields core.Fields, offsetsBySource common.OffsetsBySource, shouldSort bool) (io.WriteCloser, error) {
	sout := snappy.NewBufferedWriter(out)

	fieldStrings := make([]string, 0, len(fields))
	for _, field := range fields {
		fieldStrings = append(fieldStrings, field.String())
	}
	fieldsBytes := []byte(strings.Join(fieldStrings, fieldsDelims[CurrentFileVersion]))
	headerLength := uint32(encoding.Width64bits + len(offsetsBySource)*(encoding.Width64bits+wal.OffsetSize) + len(fieldsBytes))
	err := binary.Write(sout, encoding.Binary, headerLength)
	if err != nil {
		return nil, errors.New("Unable to write header length: %v", err)
	}
	err = fs.t.writeOffsets(sout, offsetsBySource)
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
		fs.t.db.Panic(sortErr)
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

func (rs *rowStore) writeOffsets(offsetsBySource common.OffsetsBySource) error {
	out, err := ioutil.TempFile("", "nextoffset")
	if err != nil {
		rs.t.db.Panic(err)
	}
	defer out.Close()

	err = rs.t.writeOffsets(out, offsetsBySource)
	if err != nil {
		return errors.New("Unable to write offsets: %v", err)
	}

	err = out.Sync()
	if err != nil {
		return errors.New("Unable to sync offset file: %v", err)
	}
	err = out.Close()
	if err != nil {
		return errors.New("Unable to close offset file: %v", err)
	}

	return os.Rename(out.Name(), filepath.Join(rs.opts.dir, offsetFilename))
}

func (rs *rowStore) removeOldFiles(stop <-chan interface{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			rs.t.log.Debug("Stop removing old files")
			return
		case <-ticker.C:
			files, err := listRegularFiles(rs.opts.dir)
			if err != nil {
				rs.t.log.Errorf("Unable to list data files in %v: %v", rs.opts.dir, err)
			}
			// Note - the list of files is sorted by name, which in our case is the
			// timestamp, so that means they're sorted chronologically. We don't want
			// to delete the last file in the list because that's the current one.
			foundLatest := false
			for i := len(files) - 3; i >= 0; i-- {
				filename := files[i].Name()
				if filename == offsetFilename {
					// Ignore offset file
					continue
				}
				if !foundLatest {
					foundLatest = true
					continue
				}
				rs.t.db.waitForBackupToFinish(stop)
				rs.mx.RLock()
				okayToRemove := rs.iterationsInProgress[filename] == 0 // don't remove file if we're iterating on it
				rs.mx.RUnlock()
				if okayToRemove {
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

func (fs *fileStore) iterate(outFields []core.Field, ms *memstore, okayToReuseBuffer bool, rawOkay bool, onRow func(bytemap.ByteMap, []encoding.Sequence, []byte) (more bool, err error)) (common.OffsetsBySource, error) {
	fs.t.log.Debugf("Iterating over %v", fs.filename)
	ctx := time.Now().UnixNano()
	var offsetsBySource common.OffsetsBySource

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
		fs.t.log.Debugf("No filestore available at %v, (yet), try reading the offset file", fs.filename)
		offsetFile := filepath.Join(fs.rs.opts.dir, offsetFilename)
		o, err := ioutil.ReadFile(offsetFile)
		if err != nil {
			if !os.IsNotExist(err) {
				fs.t.log.Errorf("Error reading offset file %v: %v", offsetFile, err)
			}
		} else if len(o) < wal.OffsetSize {
			fs.t.log.Errorf("Offset file contents of wrong length: %v %d", offsetFile, len(o))
		} else {
			fileVersion := FileVersion_4
			if len(o) > wal.OffsetSize {
				// Before Version 5, we only stored a single offset. Since we have more than that,
				// assume that this is at least Version 5.
				fileVersion = FileVersion_5
			}
			offsetsBySource, _ = fs.t.readOffsets(fileVersion, o)
			fs.t.log.Debugf("Set highWaterMarks from offset file: %v", offsetsBySource.TSString())
		}
	} else {
		if err != nil {
			return offsetsBySource, fs.t.log.Errorf("Unable to open file %v: %v", fs.filename, err)
		}
		fs.t.log.Debugf("Found filestore at %v", fs.filename)
		r := snappy.NewReader(file)

		var fileFields core.Fields
		offsetsBySource, _, fileFields, err = fs.info(r)
		if err != nil {
			return offsetsBySource, err
		}
		fs.t.log.Debugf("Set highWaterMark from data file: %v", offsetsBySource.TSString())

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
				return offsetsBySource, fs.t.log.Errorf("Unexpected error reading row length from %v: %v", fs.filename, err)
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
				return offsetsBySource, fs.t.log.Errorf("Unexpected error while reading row from %v: %v", fs.filename, err)
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
					fs.t.log.Errorf("Error processing row: %v", err)
					return offsetsBySource, err
				}
				continue
			}
			// At this point, we should never pass the raw data
			raw = nil

			numColumns, row := encoding.ReadInt16(row)
			colLengths := make([]int, 0, numColumns)
			for i := 0; i < numColumns; i++ {
				if len(row) < 8 {
					return offsetsBySource, fs.t.log.Errorf("Not enough data left to decode column %d length on row of length %d from %v!", i, rowLength, fs.filename)
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
					return offsetsBySource, fs.t.log.Errorf("Not enough data left to decode column from %v, wanted %d have %d", fs.filename, colLength, len(row))
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
				if err != nil {
					fs.t.log.Errorf("Error processing row from %v: %v", fs.filename, err)
				}
			}

			if !more || err != nil {
				return offsetsBySource, err
			}
		}
	}

	// Read remaining stuff from memstore
	if ms != nil {
		offsetsBySource = offsetsBySource.Advance(ms.offsetsBySource)
		ms.tree.Walk(ctx, func(key []byte, msColumns []encoding.Sequence) (bool, bool, error) {
			columns := make([]encoding.Sequence, len(outFields))
			for i, msColumn := range msColumns {
				memToOut(columns, i, msColumn)
			}
			more, err := onRow(bytemap.ByteMap(key), columns, nil)
			return more, false, err
		})
	}

	return offsetsBySource, nil
}

func (fs *fileStore) info(r io.Reader) (common.OffsetsBySource, string, core.Fields, error) {
	var offsetsBySource common.OffsetsBySource
	fileVersion := fs.t.versionFor(fs.filename)
	// File contains header with field info, use it
	headerLength := uint32(0)
	lengthErr := binary.Read(r, encoding.Binary, &headerLength)
	if lengthErr != nil {
		return offsetsBySource, "", nil, fs.t.log.Errorf("Unexpected error reading header length from %v: %v", fs.filename, lengthErr)
	}
	fieldsBytes := make([]byte, headerLength)
	_, readErr := io.ReadFull(r, fieldsBytes)
	if readErr != nil {
		return offsetsBySource, "", nil, fs.t.log.Errorf("Unable to read fields from %v: %v", fs.filename, readErr)
	}
	offsetsBySource, fieldsBytes = fs.t.readOffsets(fileVersion, fieldsBytes)
	delim := fieldsDelims[fileVersion]
	fieldsString := string(fieldsBytes)
	fieldStrings := strings.Split(fieldsString, delim)
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

	return offsetsBySource, fieldsString, fileFields, nil
}

func (fs *fileStore) markCorrupted() error {
	dir, file := filepath.Split(fs.filename)
	corruptedDir := filepath.Join(dir, "corrupted")
	corruptedFile := filepath.Join(corruptedDir, file)

	err := os.MkdirAll(corruptedDir, 0755)
	if err != nil {
		return errors.New("Unable to make corrupted subdirectory %v: %v", corruptedDir, err)
	}

	err = os.Rename(fs.filename, corruptedFile)
	if err != nil {
		return errors.New("Unable to move corrupted filestore %v to %v: %v", fs.filename, corruptedFile, err)
	}

	return nil
}

func (t *table) versionFor(filename string) int {
	fileVersion := 0
	parts := strings.Split(filepath.Base(filename), "_")
	if len(parts) == 3 {
		versionString := strings.Split(parts[2], ".")[0]
		var versionErr error
		fileVersion, versionErr = strconv.Atoi(versionString)
		if versionErr != nil {
			t.db.Panic(errors.New("Unable to determine file version for file %v: %v", filename, versionErr))
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

func (t *table) writeOffsets(file io.Writer, offsetsBySource common.OffsetsBySource) error {
	t.log.Debugf("Writing offsets: %v", offsetsBySource)
	numOffsets := make([]byte, encoding.Width64bits)
	encoding.WriteInt64(numOffsets, len(offsetsBySource))

	_, err := file.Write(numOffsets)
	if err != nil {
		return err
	}

	for source, offset := range offsetsBySource {
		sourceBytes := make([]byte, encoding.Width64bits)
		encoding.WriteInt64(sourceBytes, source)
		_, err := file.Write(sourceBytes)
		if err != nil {
			return err
		}
		_, err = file.Write(offset)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *table) readOffsets(fileVersion int, header []byte) (common.OffsetsBySource, []byte) {
	offsetsBySource := make(common.OffsetsBySource)
	defer func() {
		t.log.Debugf("Read offsets: %v", offsetsBySource)
	}()
	if fileVersion < FileVersion_5 {
		// Header contains only a single offset
		offsetsBySource[0] = wal.Offset(header[:wal.OffsetSize])
		return offsetsBySource, header[wal.OffsetSize:]
	} else {
		// Header contains multiple offsets
		var numOffsets, source int
		numOffsets, header = encoding.ReadInt64(header)
		for i := 0; i < numOffsets; i++ {
			source, header = encoding.ReadInt64(header)
			offset := header[:wal.OffsetSize]
			header = header[wal.OffsetSize:]
			offsetsBySource[source] = offset
		}
		return offsetsBySource, header
	}

}

func listRegularFiles(dir string) ([]os.FileInfo, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	regularFiles := make([]os.FileInfo, 0, len(files))
	for _, file := range files {
		if !file.IsDir() {
			regularFiles = append(regularFiles, file)
		}
	}
	return regularFiles, nil
}

func calcShaSum(filename string) (string, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return "", errors.New("Unable to open %v to calculate sha256: %v", filename, err)
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", errors.New("Unable to calculate sha256 for %v: %v", filename, err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
