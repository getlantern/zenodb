package zenodb

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/golog"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/sql"
)

// TableStats presents statistics for a given table (currently only since the
// last time the database process was started).
type TableStats struct {
	FilteredPoints int64
	QueuedPoints   int64
	InsertedPoints int64
	DroppedPoints  int64
	ExpiredValues  int64
}

// TableOpts configures a table.
type TableOpts struct {
	// Name is the name of the table.
	Name string
	// View indicates if this table is a view on top of an existing table.
	View bool
	// MinFlushLatency sets a lower bound on how frequently the memstore is
	// flushed to disk.
	MinFlushLatency time.Duration
	// MaxFlushLatency sets an upper bound on how long to wait before flushing the
	// memstore to disk.
	MaxFlushLatency time.Duration
	// RetentionPeriod limits how long data is kept in the table (based on the
	// timestamp of the data itself).
	RetentionPeriod time.Duration
	// Backfill limits how far back to grab data from the WAL when first creating
	// a table. If 0, backfill is limited only by the RetentionPeriod.
	Backfill time.Duration
	// PartitionBy can be used in clustered deployments to decide which
	// dimensions to use in partitioning data. If unspecified, all dimensions are
	// used for partitioning.
	PartitionBy []string
	// SQL is the SELECT query that determines the fields, filtering and input
	// source for this table.
	SQL string
	// Virtual, if true, means that the table's data isn't actually stored or
	// queryable. Virtual tables are useful for defining a base set of fields
	// from which other tables can select.
	Virtual      bool
	dependencyOf []*TableOpts
}

type table struct {
	*TableOpts
	sql.Query
	fields              core.Fields
	db                  *DB
	rowStore            *rowStore
	log                 golog.Logger
	fieldsMutex         sync.RWMutex
	whereMutex          sync.RWMutex
	stats               TableStats
	statsMutex          sync.RWMutex
	wal                 *wal.Reader
	readOffset          wal.Offset
	iterations          *iteration
	highWaterMarkDisk   int64
	highWaterMarkMemory int64
	highWaterMarkMx     sync.RWMutex
}

type iteration struct {
	t               *table
	ctx             context.Context
	outFields       core.Fields
	includeMemStore bool
	onValue         func(bytemap.ByteMap, []encoding.Sequence) (more bool, err error)
	fieldMappings   map[int]int
	errCh           chan error
}

// CreateTable creates a table based on the given opts.
func (db *DB) CreateTable(opts *TableOpts) error {
	q, fields, err := db.queryAndFields(opts)
	if err != nil {
		return err
	}

	if db.opts.ReadOnly {
		// treat all tables as Virtual on read-only databases
		opts.Virtual = true
	}

	if !opts.Virtual {
		if opts.RetentionPeriod <= 0 {
			return errors.New("Please specify a positive RetentionPeriod")
		}
		if opts.MinFlushLatency <= 0 {
			log.Debug("MinFlushLatency disabled")
		}
		if opts.MaxFlushLatency <= 0 {
			opts.MaxFlushLatency = time.Duration(math.MaxInt64)
			log.Debug("MaxFlushLatency disabled")
		}
	}
	opts.Name = strings.ToLower(opts.Name)

	t := &table{
		TableOpts: opts,
		Query:     *q,
		fields:    fields,
		db:        db,
		log:       golog.LoggerFor("zenodb." + opts.Name),
	}

	t.log.Debugf("Fields will be: %v", fields)
	t.applyWhere(q.Where)

	var rsErr error
	var walOffset wal.Offset
	if !t.Virtual {
		t.rowStore, walOffset, rsErr = t.openRowStore(&rowStoreOptions{
			dir:             filepath.Join(db.opts.Dir, t.Name),
			minFlushLatency: t.MinFlushLatency,
			maxFlushLatency: t.MaxFlushLatency,
		})
		if rsErr != nil {
			return rsErr
		}

		offsetByRetentionPeriod := wal.NewOffsetForTS(t.truncateBefore())
		if offsetByRetentionPeriod.After(walOffset) {
			// Don't bother looking further back than table's retention period
			walOffset = offsetByRetentionPeriod
		}

		offsetByBackfillDepth := wal.NewOffsetForTS(t.backfillTo())
		if offsetByBackfillDepth.After(walOffset) {
			// Don't bother looking further back than table's backfill depth
			walOffset = offsetByBackfillDepth
		}

		t.log.Debugf("Starting at WAL offset %v", walOffset)
	}

	db.tablesMutex.Lock()
	defer db.tablesMutex.Unlock()

	if db.tables[t.Name] != nil {
		return fmt.Errorf("Table %v already exists", t.Name)
	}
	db.tables[t.Name] = t
	db.orderedTables = append(db.orderedTables, t)

	if !t.Virtual {
		if !t.db.opts.Passthrough {
			go t.logHighWaterMark()
		}

		if t.db.opts.Follow != nil {
			t.startFollowing(walOffset)
			return nil
		}
		return t.startWALProcessing(walOffset)
	}

	return nil
}

func (t *table) Alter(opts *TableOpts) error {
	q, fields, err := t.db.queryAndFields(opts)
	if err != nil {
		return err
	}
	t.applyWhere(q.Where)
	t.applyFields(fields)
	return nil
}

func (db *DB) queryAndFields(opts *TableOpts) (q *sql.Query, fields core.Fields, err error) {
	q, err = sql.Parse(opts.SQL)
	if err != nil {
		return
	}
	if !opts.View {
		fields, err = q.Fields.Get(nil)
	} else {
		// It's a view

		// Get existing fields from existing table
		t := db.getTable(q.From)
		if t == nil {
			err = fmt.Errorf("Table '%v' not found", t.Name)
			return
		}

		// Point view at same stream as table
		// TODO: populate view with existing data from table
		q.From = t.From

		if q.GroupBy == nil {
			q.GroupBy = t.GroupBy
			q.GroupByAll = t.GroupByAll
		}

		if q.Resolution == 0 {
			q.Resolution = t.Resolution
		}

		if len(opts.PartitionBy) == 0 {
			opts.PartitionBy = t.PartitionBy
		}

		// Combine where clauses
		if t.Where != nil {
			if q.Where == nil {
				q.Where = t.Where
			} else {
				combined, binaryErr := goexpr.Binary("AND", q.Where, t.Where)
				if binaryErr != nil {
					err = binaryErr
					return
				}
				q.Where = combined
			}
		}

		fields, err = q.Fields.Get(t.getFields())
	}

	if err == nil {
		fields = addPointsField(fields)
	}

	return
}

func addPointsField(fields core.Fields) core.Fields {
	for _, field := range fields {
		if field.Equals(core.PointsField) {
			// already have _points field, nothing to do
			return fields
		}
	}

	// prepend the magic _points field
	newFields := make([]core.Field, 0, len(fields)+1)
	newFields = append(newFields, core.PointsField)
	newFields = append(newFields, fields...)
	return newFields
}

func (t *table) startFollowing(walOffset wal.Offset) {
	newSubscriber := t.db.newStreamSubscriber[t.From]
	if newSubscriber == nil {
		newSubscriber = make(chan *tableWithOffset, 100)
		go t.db.followLeader(t.From, newSubscriber)
		t.db.newStreamSubscriber[t.From] = newSubscriber
	}
	newSubscriber <- &tableWithOffset{t, walOffset}
}

func (t *table) startWALProcessing(walOffset wal.Offset) error {
	var walErr error
	w := t.db.streams[t.From]
	if w == nil {
		walDir := filepath.Join(t.db.opts.Dir, "_wal", t.From)
		dirErr := os.MkdirAll(walDir, 0755)
		if dirErr != nil && !os.IsExist(dirErr) {
			return dirErr
		}
		w, walErr = wal.Open(walDir, t.db.opts.WALSyncInterval)
		if walErr != nil {
			return walErr
		}
		go t.db.capWALAge(w)
		t.db.streams[t.From] = w
	}

	if t.db.opts.Passthrough {
		t.log.Debugf("Passthrough will not insert data to table %v", t.Name)
		return nil
	}

	t.log.Debugf("Will read inserts from %v at offset %v", t.From, walOffset)
	t.wal, walErr = w.NewReader(t.Name, walOffset, t.db.walBuffers.Get)
	if walErr != nil {
		return fmt.Errorf("Unable to obtain WAL reader: %v", walErr)
	}

	go t.processWALInserts()
	return nil
}

func (t *table) applyFields(fields core.Fields) {
	var fieldsChanged bool
	t.fieldsMutex.Lock()
	fieldsChanged = !fields.Equals(t.fields)
	if fieldsChanged {
		t.fields = fields
	}
	t.fieldsMutex.Unlock()
	if fieldsChanged {
		if !t.Virtual && !t.db.opts.Passthrough {
			t.rowStore.fieldUpdates <- fields
		}
		t.log.Debugf("Updated fields to %v", fields)
	} else {
		t.log.Debug("Fields unchanged")
	}
}

func (t *table) getFields() core.Fields {
	t.fieldsMutex.RLock()
	fields := make(core.Fields, len(t.fields))
	copy(fields, t.fields)
	t.fieldsMutex.RUnlock()
	return fields
}

func (t *table) applyWhere(where goexpr.Expr) {
	var whereChanged bool
	t.whereMutex.Lock()
	whereChanged = t.Where != where
	t.Where = where
	t.whereMutex.Unlock()
	if whereChanged {
		t.log.Debugf("Updated where to %v", where)
	} else {
		t.log.Debug("Where unchanged")
	}
}

func (t *table) getWhere() goexpr.Expr {
	t.whereMutex.RLock()
	where := t.Where
	t.whereMutex.RUnlock()
	return where
}

func (t *table) truncateBefore() time.Time {
	return t.db.clock.Now().Add(-1 * t.RetentionPeriod)
}

func (t *table) backfillTo() time.Time {
	if t.Backfill == 0 {
		return time.Time{}
	}
	return t.db.clock.Now().Add(-1 * t.Backfill)
}

func (t *table) iterate(ctx context.Context, outFields core.Fields, includeMemStore bool, onValue func(bytemap.ByteMap, []encoding.Sequence) (more bool, err error)) error {
	it := &iteration{
		t:               t,
		ctx:             ctx,
		outFields:       outFields,
		includeMemStore: includeMemStore,
		onValue:         onValue,
		errCh:           make(chan error, 1),
	}
	t.db.requestedIterations <- it
	return <-it.errCh
}

// coalesceIterations coalesces multiple parallel iterations for a single table,
// which avoids having to do multiple table scans of the same table.
func (db *DB) coalesceIterations() {
	for it := range db.requestedIterations {
		db.coalesceIteration(it)
	}
}

func (db *DB) coalesceIteration(it *iteration) {
	iterations := append([]*iteration(nil), it)
	iterationsForOtherTables := make([]*iteration, 0)

coalesceLoop:
	for {
		select {
		case it2 := <-db.requestedIterations:
			if it2.t == it.t {
				iterations = append(iterations, it2)
			} else {
				iterationsForOtherTables = append(iterationsForOtherTables, it2)
			}
		case <-time.After(db.opts.IterationCoalesceInterval):
			// stop waiting to coalesce
			break coalesceLoop
		}
	}

	// re-enqueue iterations for other tables since we won't be handling them
	// here
	for _, otherIt := range iterationsForOtherTables {
		db.requestedIterations <- otherIt
	}

	db.coalescedIterations <- iterations
}

func (db *DB) processIterations() {
	for iterations := range db.coalescedIterations {
		db.doProcessIterations(iterations)
	}
}

func (db *DB) doProcessIterations(iterations []*iteration) {
	var maxDeadline time.Time
	includeMemStore := false
	allOutFields := make(core.Fields, 0)
	hasOutField := func(field core.Field) bool {
		for _, existingField := range allOutFields {
			if existingField.String() == field.String() {
				return true
			}
		}
		return false
	}

	for _, it := range iterations {
		includeMemStore = includeMemStore || it.includeMemStore
		deadline, hasDeadline := it.ctx.Deadline()
		if hasDeadline && deadline.After(maxDeadline) {
			maxDeadline = deadline
		}
		// default outFields to table fields
		if it.outFields == nil {
			it.outFields = it.t.fields
		}
		for _, field := range it.outFields {
			if !hasOutField(field) {
				allOutFields = append(allOutFields, field)
			}
		}
	}

	for _, it := range iterations {
		it.fieldMappings = make(map[int]int, len(allOutFields))
		for i, field := range allOutFields {
			it.fieldMappings[i] = it.indexOfOutField(field)
		}
	}

	log.Debugf("Coalescing %d iterations on %v", len(iterations), iterations[0].t.Name)

	remainingIterations := make(map[int]*iteration, len(iterations))
	for i, it := range iterations {
		remainingIterations[i] = it
	}

	combinedOnValue := func(dims bytemap.ByteMap, vals []encoding.Sequence) (bool, error) {
		more := false
		for i, it := range remainingIterations {
			itVals := make([]encoding.Sequence, len(it.outFields))
			for i, val := range vals {
				itI := it.fieldMappings[i]
				if itI >= 0 {
					itVals[itI] = val
				}
			}
			itMore, err := it.onValue(dims, itVals)
			if err != nil {
				log.Errorf("Error while iterating: %v", err)
				return false, err
			}
			if !itMore {
				// This iteration doesn't want any more data, stop feeding it
				delete(remainingIterations, i)
			} else {
				more = true
			}
		}
		return more, nil
	}

	newCtx := context.Background()
	if !maxDeadline.IsZero() {
		var cancel context.CancelFunc
		newCtx, cancel = context.WithDeadline(newCtx, maxDeadline)
		defer cancel()
	}
	err := iterations[0].t.rowStore.iterate(newCtx, allOutFields, includeMemStore, combinedOnValue)
	for _, it := range iterations {
		it.errCh <- err
	}
}

func (it *iteration) indexOfOutField(field core.Field) int {
	for i, existingField := range it.outFields {
		if existingField.String() == field.String() {
			return i
		}
	}
	return -1
}

// shouldSort determines whether or not a flush should be sorted. The flush will
// sort if the table is the next table in line to be sorted, and no other sort
// is currently happening. If shouldSort returns true, the flushing process
// must call stopSorting when finished so that other tables have a chance to
// sort.
func (t *table) shouldSort() bool {
	if t.db.opts.MaxMemoryRatio <= 0 {
		return false
	}

	t.db.tablesMutex.RLock()
	if t.db.nextTableToSort >= len(t.db.orderedTables) {
		t.db.nextTableToSort = 0
	}
	nextTableToSort := t.db.orderedTables[t.db.nextTableToSort]
	result := t.Name == nextTableToSort.Name && !t.db.isSorting
	t.db.tablesMutex.RUnlock()
	return result
}

func (t *table) stopSorting() {
	t.db.tablesMutex.RLock()
	t.db.isSorting = false
	t.db.nextTableToSort++
	t.db.tablesMutex.RUnlock()
}

func (t *table) memStoreSize() int {
	return t.rowStore.memStoreSize()
}

func (t *table) forceFlush() {
	t.rowStore.forceFlush()
}

func (t *table) logHighWaterMark() {
	for {
		time.Sleep(15 * time.Second)
		t.highWaterMarkMx.RLock()
		disk := t.highWaterMarkDisk
		memory := t.highWaterMarkMemory
		t.highWaterMarkMx.RUnlock()
		t.log.Debugf("High Water Mark    disk: %v    memory: %v", encoding.TimeFromInt(disk).In(time.UTC), encoding.TimeFromInt(memory).In(time.UTC))
	}
}

func (t *table) updateHighWaterMarkDisk(ts int64) {
	t.highWaterMarkMx.Lock()
	if ts > t.highWaterMarkDisk {
		t.highWaterMarkDisk = ts
	}
	t.highWaterMarkMx.Unlock()
}

func (t *table) updateHighWaterMarkMemory(ts int64) {
	t.highWaterMarkMx.Lock()
	if ts > t.highWaterMarkMemory {
		t.highWaterMarkMemory = ts
	}
	t.highWaterMarkMx.Unlock()
}
