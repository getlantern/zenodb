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
	"github.com/getlantern/zenodb/expr"
	"github.com/getlantern/zenodb/sql"
)

type entry struct {
	dims       map[string]interface{}
	fields     []sequence
	havingTest sequence
}

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
	Table         string
	AsOf          time.Time
	Until         time.Time
	Resolution    time.Duration
	FieldNames    []string // FieldNames are needed for serializing QueryResult across rpc
	GroupBy       []string
	Rows          []*Row
	Stats         *QueryStats
	NumPeriods    int
	ScannedPoints int64
}

type Query struct {
	db *DB
	sql.Query
}

type queryResponse struct {
	key         bytemap.ByteMap
	field       string
	e           expr.Expr
	seq         sequence
	startOffset int
}

type subMergeSpec struct {
	idx      int
	field    sql.Field
	subMerge expr.SubMerge
}

type queryExecution struct {
	db *DB
	sql.Query
	t                *table
	q                *query
	subMergers       [][]expr.SubMerge
	havingSubMergers []expr.SubMerge
	dimsMap          map[string]bool
	scalingFactor    int
	inPeriods        int
	outPeriods       int
	numWorkers       int
	responsesCh      chan *queryResponse
	entriesCh        chan map[string]*entry
	wg               sync.WaitGroup
	scannedPoints    int64
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
		groupBy := make([]string, len(exec.GroupBy))
		copy(groupBy, exec.GroupBy)
		sort.Strings(groupBy)
		sliceKey = func(key bytemap.ByteMap) bytemap.ByteMap {
			return key.Slice(groupBy...)
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
					fields: make([]sequence, len(exec.Fields)),
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

				// Initialize fields
				for i, f := range exec.Fields {
					seq := newSequence(f.Expr.EncodedWidth(), exec.outPeriods)
					seq.setStart(exec.q.until)
					en.fields[i] = seq
				}

				// Initialize havings
				if exec.Having != nil {
					en.havingTest = newSequence(exec.Having.EncodedWidth(), exec.outPeriods)
				}

				entries[string(kb)] = en
			}

			inPeriods := resp.seq.numPeriods(resp.e.EncodedWidth()) - resp.startOffset
			for c, column := range exec.t.Fields {
				if column.Name != resp.field {
					continue
				}
				for t := 0; t < inPeriods && t < exec.inPeriods; t++ {
					other, wasSet := resp.seq.dataAt(t+resp.startOffset, resp.e)
					if !wasSet {
						continue
					}
					atomic.AddInt64(&exec.scannedPoints, 1)

					out := t / exec.scalingFactor
					for f, field := range exec.Fields {
						subMerge := exec.subMergers[f][c]
						if subMerge == nil {
							continue
						}
						seq := en.fields[f]
						seq.subMergeValueAt(out, field.Expr, subMerge, other, resp.key)
					}

					// Calculate havings
					if exec.Having != nil {
						subMerge := exec.havingSubMergers[c]
						if subMerge == nil {
							continue
						}
						en.havingTest.subMergeValueAt(out, exec.Having, subMerge, other, resp.key)
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

	exec.q.onValues = func(key bytemap.ByteMap, field string, e expr.Expr, seq sequence, startOffset int) {
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
		exec.GroupBy = make([]string, 0, len(exec.dimsMap))
		for dim := range exec.dimsMap {
			exec.GroupBy = append(exec.GroupBy, dim)
		}
		sort.Strings(exec.GroupBy)
	}

	rows := exec.sortRows(exec.mergedRows())

	result := &QueryResult{
		Table:         exec.From,
		AsOf:          exec.q.asOf,
		Until:         exec.q.until,
		Resolution:    exec.Resolution,
		FieldNames:    make([]string, 0, len(exec.Fields)),
		GroupBy:       exec.GroupBy,
		NumPeriods:    exec.outPeriods,
		Rows:          rows,
		Stats:         stats,
		ScannedPoints: exec.scannedPoints,
	}
	for _, field := range exec.Fields {
		result.FieldNames = append(result.FieldNames, field.Name)
	}

	return result, nil
}

func (exec *queryExecution) mergedRows() []*Row {
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
							ex := exec.Fields[x].Expr
							res := v.fields[x].merge(os, ex, exec.Resolution, exec.AsOf)
							if log.IsTraceEnabled() {
								log.Tracef("Merging %v ->\n\t%v yielded\n\t%v", os.String(ex), v.fields[x].String(ex), res.String(ex))
							}
							v.fields[x] = res
						}
						if exec.Having != nil {
							v.havingTest = v.havingTest.merge(vo.havingTest, exec.Having, exec.Resolution, exec.AsOf)
						}
						delete(o, k)
					}
				}
			}

			dims := make([]interface{}, 0, len(exec.dimsMap))
			for _, dim := range exec.GroupBy {
				dims = append(dims, v.dims[dim])
			}
			for t := 0; t < exec.outPeriods; t++ {
				if exec.Having != nil {
					testResult, ok := v.havingTest.ValueAt(t, exec.Having)
					if !ok || int(testResult) != 1 {
						// Didn't meet having criteria, ignore
						continue
					}
				}
				values := make([]float64, 0, len(exec.Fields))
				hasData := false
				for i, field := range exec.Fields {
					vals := v.fields[i]
					val, wasSet := vals.ValueAt(t, field.Expr)
					values = append(values, val)
					if wasSet {
						hasData = true
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
					groupBy: exec.GroupBy,
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
		if va == nil {
			if vb != nil {
				return true
			}
			continue
		}
		if vb == nil {
			if va != nil {
				return false
			}
			continue
		}
		switch tva := va.(type) {
		case bool:
			tvb := vb.(bool)
			if tva && !tvb {
				return false
			}
			if !tva && tvb {
				return true
			}
		case byte:
			tvb := vb.(byte)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case uint16:
			tvb := vb.(uint16)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case uint32:
			tvb := vb.(uint32)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case uint64:
			tvb := vb.(uint64)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case uint:
			tvb := uint(vb.(uint64))
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case int8:
			tvb := vb.(int8)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case int16:
			tvb := vb.(int16)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case int32:
			tvb := vb.(int32)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case int64:
			tvb := vb.(int64)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case int:
			tvb := vb.(int)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case float32:
			tvb := vb.(float32)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case float64:
			tvb := vb.(float64)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case string:
			tvb := vb.(string)
			if tva > tvb {
				return false
			}
			if tva < tvb {
				return true
			}
		case time.Time:
			tvb := vb.(time.Time)
			if tva.After(tvb) {
				return false
			}
			if tva.Before(tvb) {
				return true
			}
		}
	}
	return false
}
