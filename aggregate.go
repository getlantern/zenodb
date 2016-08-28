package zenodb

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/goexpr"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/sql"
)

type Row struct {
	Period  int
	Dims    []interface{}
	Values  []float64
	groupBy []string
	fields  []sql.Field
}

// Get implements the interface method from goexpr.Params
func (row *Row) get(param string) interface{} {
	// First look at fields
	for i, field := range row.fields {
		if field.Name == param {
			return row.Values[i]
		}
	}

	// Then look at dims
	for i, dim := range row.groupBy {
		if dim == param {
			return row.Dims[i]
		}
	}

	return nil
}

type QueryResult struct {
	Table            string
	AsOf             time.Time
	Until            time.Time
	Resolution       time.Duration
	FieldNames       []string // FieldNames are needed for serializing QueryResult across rpc
	IsCrosstab       bool
	CrosstabDims     []interface{}
	GroupBy          []string
	PopulatedColumns []bool
	Rows             []*Row
	Stats            *QueryStats
	NumPeriods       int
	ScannedPoints    int64
}

type Query struct {
	db *DB
	sql.Query
}

type queryResponse struct {
	key         bytemap.ByteMap
	field       string
	e           expr.Expr
	seq         encoding.Sequence
	startOffset int
}

type subMergeSpec struct {
	idx      int
	field    sql.Field
	subMerge expr.SubMerge
}

// entry is an intermediary data holder for aggregating data during the
// execution of a query.
type entry struct {
	dims       map[string]interface{}
	fields     []encoding.Sequence
	havingTest encoding.Sequence
}

type queryExecution struct {
	db *DB
	sql.Query
	t                      *table
	q                      *query
	subMergers             [][]expr.SubMerge
	havingSubMergers       []expr.SubMerge
	dimsMap                map[string]bool
	isCrosstab             bool
	crosstabDims           []interface{}
	crosstabDimIdxs        map[interface{}]int
	crosstabDimReverseIdxs []int
	populatedColumns       []bool
	scalingFactor          int
	inPeriods              int
	outPeriods             int
	numWorkers             int
	responsesCh            chan *queryResponse
	entriesCh              chan map[string]*entry
	wg                     sync.WaitGroup
	scannedPoints          int64
}

func (db *DB) SQLQuery(sqlString string) (*Query, error) {
	table, err := sql.TableFor(sqlString)
	if err != nil {
		return nil, err
	}
	t := db.getTable(table)
	if t == nil {
		return nil, fmt.Errorf("Table '%v' not found", table)
	}

	query, err := sql.Parse(sqlString, t.Fields...)
	if err != nil {
		return nil, err
	}
	return db.Query(query), nil
}

func (db *DB) Query(query *sql.Query) *Query {
	return &Query{db: db, Query: *query}
}

func (aq *Query) Run() (*QueryResult, error) {
	q := &query{
		table:       aq.From,
		asOf:        aq.AsOf,
		asOfOffset:  aq.AsOfOffset,
		until:       aq.Until,
		untilOffset: aq.UntilOffset,
	}
	numWorkers := runtime.NumCPU()
	exec := &queryExecution{
		Query:       aq.Query,
		db:          aq.db,
		t:           aq.db.getTable(aq.From),
		q:           q,
		numWorkers:  numWorkers,
		responsesCh: make(chan *queryResponse, numWorkers),
		entriesCh:   make(chan map[string]*entry, numWorkers),
	}
	exec.wg.Add(numWorkers)
	return exec.run()
}

func (exec *queryExecution) run() (*QueryResult, error) {
	err := exec.prepare()
	if err != nil {
		return nil, err
	}
	return exec.finish()
}

func (exec *queryExecution) prepare() error {
	exec.isCrosstab = exec.Crosstab != nil
	if exec.isCrosstab {
		exec.crosstabDimIdxs = make(map[interface{}]int, 0)
	}

	// Figure out what to select
	columns := make([]expr.Expr, 0, len(exec.t.Fields))
	for _, column := range exec.t.Fields {
		columns = append(columns, column.Expr)
	}
	includedColumns := make(map[int]bool)

	var subMergers [][]expr.SubMerge
	for _, field := range exec.Fields {
		sms := field.Expr.SubMergers(columns)
		subMergers = append(subMergers, sms)
		columnFound := false
		for j, sm := range sms {
			if sm != nil {
				columnFound = true
				includedColumns[j] = true
			}
		}
		if !columnFound {
			return fmt.Errorf("No column found for %v", field.String())
		}
	}

	var havingSubMergers []expr.SubMerge
	if exec.Having != nil {
		havingSubMergers = exec.Having.SubMergers(columns)
		for j, sm := range havingSubMergers {
			if sm != nil {
				includedColumns[j] = true
			}
		}
	}

	var fields []string
	for i, column := range exec.t.Fields {
		if includedColumns[i] {
			fields = append(fields, column.Name)
		}
	}
	exec.q.fields = fields
	exec.subMergers = subMergers
	exec.havingSubMergers = havingSubMergers

	if exec.Where != nil {
		log.Tracef("Applying where: %v", exec.Where)
		exec.q.filter = exec.Where
	}

	err := exec.q.init(exec.db)
	if err != nil {
		return err
	}

	nativeResolution := exec.t.Resolution
	if exec.Resolution == 0 {
		log.Trace("Defaulting to native resolution")
		exec.Resolution = nativeResolution
	}
	if exec.Resolution > exec.t.RetentionPeriod {
		log.Trace("Not allowing resolution lower than retention period")
		exec.Resolution = exec.t.RetentionPeriod
	}
	if exec.Resolution < nativeResolution {
		return fmt.Errorf("Query's resolution of %v is higher than table's native resolution of %v", exec.Resolution, nativeResolution)
	}
	if exec.Resolution%nativeResolution != 0 {
		return fmt.Errorf("Query's resolution of %v is not evenly divisible by the table's native resolution of %v", exec.Resolution, nativeResolution)
	}

	exec.scalingFactor = int(exec.Resolution / nativeResolution)
	log.Tracef("Scaling factor: %d", exec.scalingFactor)

	exec.inPeriods = int(exec.q.until.Sub(exec.q.asOf) / nativeResolution)
	// Limit inPeriods based on what we can fit into outPeriods
	exec.inPeriods -= exec.inPeriods % exec.scalingFactor
	exec.outPeriods = exec.inPeriods / exec.scalingFactor
	if exec.outPeriods == 0 {
		exec.outPeriods = 1
	}
	exec.inPeriods = exec.outPeriods * exec.scalingFactor
	log.Tracef("In: %d   Out: %d", exec.inPeriods, exec.outPeriods)

	var dimsMapMutex sync.Mutex
	var sliceKey func(key bytemap.ByteMap) bytemap.ByteMap
	if exec.GroupByAll {
		// Use all original dimensions in grouping
		exec.dimsMap = make(map[string]bool, 0)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			// Original key is fine
			return key
		}
	} else if len(exec.GroupBy) == 0 {
		defaultKey := bytemap.New(map[string]interface{}{
			"default": "",
		})
		// No grouping, put everything under a single key
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return defaultKey
		}
	} else {
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			names := make([]string, 0, len(exec.GroupBy))
			values := make([]interface{}, 0, len(exec.GroupBy))
			for _, groupBy := range exec.GroupBy {
				val := groupBy.Expr.Eval(key)
				if val != nil {
					names = append(names, groupBy.Name)
					values = append(values, val)
				}
			}
			return bytemap.FromSortedKeysAndValues(names, values)
		}
	}

	worker := func() {
		entries := make(map[string]*entry, 0)
		for resp := range exec.responsesCh {
			kb := sliceKey(resp.key)
			en := entries[string(kb)]
			if en == nil {
				en = &entry{
					dims:   kb.AsMap(),
					fields: make([]encoding.Sequence, len(exec.Fields)),
				}

				if exec.GroupByAll {
					// Track dims
					for dim := range en.dims {
						// TODO: instead of locking on this shared state, have the workers
						// return their own dimsMaps and merge them
						dimsMapMutex.Lock()
						exec.dimsMap[dim] = true
						dimsMapMutex.Unlock()
					}
				}

				// Initialize havings
				if exec.Having != nil {
					en.havingTest = encoding.NewSequence(exec.Having.EncodedWidth(), exec.outPeriods)
				}

				entries[string(kb)] = en
			}

			inPeriods := resp.seq.NumPeriods(resp.e.EncodedWidth()) - resp.startOffset
			for c, column := range exec.t.Fields {
				if column.Name != resp.field {
					continue
				}
				for t := 0; t < inPeriods && t < exec.inPeriods; t++ {
					other, wasSet := resp.seq.DataAt(t+resp.startOffset, resp.e)
					if !wasSet {
						continue
					}
					atomic.AddInt64(&exec.scannedPoints, 1)

					crosstabDimIdx := 0
					if exec.isCrosstab {
						crosstabDim := exec.Crosstab.Eval(resp.key)
						dimsMapMutex.Lock()
						var found bool
						crosstabDimIdx, found = exec.crosstabDimIdxs[crosstabDim]
						if !found {
							numCrosstabDims := len(exec.crosstabDims)
							crosstabFull := numCrosstabDims >= 1000
							if crosstabFull {
								dimsMapMutex.Unlock()
								continue
							}
							crosstabDimIdx = numCrosstabDims
							exec.crosstabDimIdxs[crosstabDim] = crosstabDimIdx
							exec.crosstabDims = append(exec.crosstabDims, crosstabDim)
						}
						dimsMapMutex.Unlock()
					}

					out := t / exec.scalingFactor
					for f, field := range exec.Fields {
						subMerge := exec.subMergers[f][c]
						if subMerge == nil {
							continue
						}

						idx := f
						if exec.isCrosstab {
							idx = crosstabDimIdx*len(exec.Fields) + f
						}
						if idx >= len(en.fields) {
							// Grow fields
							orig := en.fields
							en.fields = make([]encoding.Sequence, idx+1)
							copy(en.fields, orig)
						}
						seq := en.fields[idx]
						if seq == nil {
							// Lazily initialize sequence
							seq = encoding.NewSequence(field.Expr.EncodedWidth(), exec.outPeriods)
							seq.SetStart(exec.q.until)
							en.fields[idx] = seq
						}
						seq.SubMergeValueAt(out, field.Expr, subMerge, other, resp.key)
					}

					// Calculate havings
					if exec.Having != nil {
						subMerge := exec.havingSubMergers[c]
						if subMerge == nil {
							continue
						}
						en.havingTest.SubMergeValueAt(out, exec.Having, subMerge, other, resp.key)
					}
				}
			}
		}

		exec.entriesCh <- entries
		exec.wg.Done()
	}

	for i := 0; i < exec.numWorkers; i++ {
		go worker()
	}

	exec.q.onValues = func(key bytemap.ByteMap, field string, e expr.Expr, seq encoding.Sequence, startOffset int) {
		exec.responsesCh <- &queryResponse{key, field, e, seq, startOffset}
	}

	return nil
}

func (exec *queryExecution) finish() (*QueryResult, error) {
	stats, err := exec.q.run(exec.db)
	if err != nil {
		return nil, err
	}
	close(exec.responsesCh)
	exec.wg.Wait()
	close(exec.entriesCh)
	if log.IsTraceEnabled() {
		log.Tracef("%v\nScanned Points: %v", spew.Sdump(stats), humanize.Comma(exec.scannedPoints))
	}

	if len(exec.GroupBy) == 0 {
		// Fill in group by based on dims discovered during query
		dims := make([]string, 0, len(exec.dimsMap))
		for dim := range exec.dimsMap {
			dims = append(dims, dim)
		}
		sort.Strings(dims)
		for _, dim := range dims {
			exec.GroupBy = append(exec.GroupBy, sql.NewGroupBy(dim, goexpr.Param(dim)))
		}
	}

	// Extract group by strings
	var groupBy []string
	for _, gb := range exec.GroupBy {
		groupBy = append(groupBy, gb.Name)
	}

	exec.crosstabDimReverseIdxs = make([]int, len(exec.crosstabDims))
	sort.Sort(orderedValues(exec.crosstabDims))
	for i, dim := range exec.crosstabDims {
		exec.crosstabDimReverseIdxs[exec.crosstabDimIdxs[dim]] = i
	}
	numColumns := len(exec.Fields)
	if exec.isCrosstab {
		numColumns = numColumns * len(exec.crosstabDims)
	}
	exec.populatedColumns = make([]bool, numColumns)
	rows := exec.sortRows(exec.mergedRows(groupBy))

	fieldNames := make([]string, 0, len(exec.Fields))
	for _, field := range exec.Fields {
		fieldNames = append(fieldNames, field.Name)
	}

	result := &QueryResult{
		Table:            exec.From,
		AsOf:             exec.q.asOf,
		Until:            exec.q.until,
		Resolution:       exec.Resolution,
		FieldNames:       fieldNames,
		IsCrosstab:       exec.isCrosstab,
		CrosstabDims:     exec.crosstabDims,
		GroupBy:          groupBy,
		NumPeriods:       exec.outPeriods,
		PopulatedColumns: exec.populatedColumns,
		Rows:             rows,
		Stats:            stats,
		ScannedPoints:    exec.scannedPoints,
	}

	return result, nil
}

func (exec *queryExecution) mergedRows(groupBy []string) []*Row {
	var entries []map[string]*entry
	for e := range exec.entriesCh {
		entries = append(entries, e)
	}

	var rows []*Row
	for i, e := range entries {
		for k, v := range e {
			for j := i; j < len(entries); j++ {
				o := entries[j]
				if j != i {
					vo, ok := o[k]
					if ok {
						for x, os := range vo.fields {
							if os != nil {
								fieldIdx := x
								if exec.isCrosstab {
									fieldIdx = x % len(exec.Fields)
								}
								ex := exec.Fields[fieldIdx].Expr
								if x >= len(v.fields) {
									// Grow
									orig := v.fields
									v.fields = make([]encoding.Sequence, x+1)
									copy(v.fields, orig)
									v.fields[x] = os
								} else {
									res := v.fields[x].Merge(os, ex, exec.Resolution, exec.AsOf)
									if log.IsTraceEnabled() {
										log.Tracef("Merging %v ->\n\t%v yielded\n\t%v", os.String(ex), v.fields[x].String(ex), res.String(ex))
									}
									v.fields[x] = res
								}
							}
							if exec.Having != nil {
								v.havingTest = v.havingTest.Merge(vo.havingTest, exec.Having, exec.Resolution, exec.AsOf)
							}
						}
						delete(o, k)
					}
				}
			}

			dims := make([]interface{}, 0, len(exec.dimsMap))
			for _, groupBy := range exec.GroupBy {
				dims = append(dims, v.dims[groupBy.Name])
			}
			for t := 0; t < exec.outPeriods; t++ {
				if exec.Having != nil {
					testResult, ok := v.havingTest.ValueAt(t, exec.Having)
					if !ok || int(testResult) != 1 {
						// Didn't meet having criteria, ignore
						continue
					}
				}
				numFields := len(exec.Fields)
				if exec.isCrosstab {
					numFields = numFields * len(exec.crosstabDims)
				}
				values := make([]float64, numFields)
				hasData := false
				for i, vals := range v.fields {
					if vals == nil {
						continue
					}
					fieldIdx := i
					outIdx := i
					if exec.isCrosstab {
						fieldIdx = i % len(exec.Fields)
						dimIdx := i / len(exec.Fields)
						sortedDimIdx := exec.crosstabDimReverseIdxs[dimIdx]
						outIdx = fieldIdx*len(exec.crosstabDims) + sortedDimIdx
					}
					field := exec.Fields[fieldIdx]
					val, wasSet := vals.ValueAt(t, field.Expr)
					values[outIdx] = val
					if wasSet {
						hasData = true
						exec.populatedColumns[outIdx] = true
					}
				}
				if !hasData {
					// Exclude rows that have no data
					// TODO: add ability to fill
					continue
				}
				rows = append(rows, &Row{
					Period:  t,
					Dims:    dims,
					Values:  values,
					groupBy: groupBy,
					fields:  exec.Fields,
				})
			}
		}
	}

	return rows
}

func (exec *queryExecution) sortRows(rows []*Row) []*Row {
	if len(exec.OrderBy) == 0 {
		return rows
	}

	ordered := &orderedRows{exec.OrderBy, rows}
	sort.Sort(ordered)
	rows = ordered.rows

	if exec.Offset > 0 {
		if exec.Offset > len(rows) {
			return make([]*Row, 0)
		}
		rows = rows[exec.Offset:]
	}

	if exec.Limit > 0 {
		if exec.Limit > len(rows) {
			return make([]*Row, 0)
		}
		rows = rows[:exec.Limit]
	}

	return rows
}

type orderedRows struct {
	orderBy []sql.Order
	rows    []*Row
}

func (r orderedRows) Len() int      { return len(r.rows) }
func (r orderedRows) Swap(i, j int) { r.rows[i], r.rows[j] = r.rows[j], r.rows[i] }
func (r orderedRows) Less(i, j int) bool {
	a := r.rows[i]
	b := r.rows[j]
	for _, order := range r.orderBy {
		// _time is a special case
		if order.Field == "_time" {
			ta := a.Period
			tb := b.Period
			if order.Descending {
				ta, tb = tb, ta
			}
			if ta > tb {
				return true
			}
			continue
		}

		// sort by field or dim
		va := a.get(order.Field)
		vb := b.get(order.Field)
		if order.Descending {
			va, vb = vb, va
		}
		result := compare(va, vb)
		if result < 0 {
			return true
		}
		if result > 0 {
			return false
		}
	}
	return false
}
