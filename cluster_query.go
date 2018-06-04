package zenodb

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/mtime"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/planner"
)

func (db *DB) RegisterQueryHandler(partition int, query planner.QueryClusterFN) {
	db.tablesMutex.Lock()
	handlersCh := db.remoteQueryHandlers[partition]
	if handlersCh == nil {
		handlersCh = make(chan planner.QueryClusterFN, db.opts.ClusterQueryConcurrency)
	}
	db.remoteQueryHandlers[partition] = handlersCh
	db.tablesMutex.Unlock()
	handlersCh <- query
}

func (db *DB) remoteQueryHandlerForPartition(partition int) planner.QueryClusterFN {
	db.tablesMutex.RLock()
	defer db.tablesMutex.RUnlock()
	select {
	case handler := <-db.remoteQueryHandlers[partition]:
		return handler
	default:
		return nil
	}
}

func (db *DB) queryForRemote(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onMetadata core.OnMetadata, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
	source, err := db.Query(sqlString, isSubQuery, subQueryResults, common.ShouldIncludeMemStore(ctx))
	if err != nil {
		return err
	}
	elapsed := mtime.Stopwatch()
	defer func() {
		log.Debugf("Processed query in %v: %v", elapsed(), sqlString)
	}()
	if unflat {
		return core.UnflattenOptimized(source).Iterate(ctx, onMetadata, onRow)
	}
	return source.Iterate(ctx, onMetadata, onFlatRow)
}

type remoteResult struct {
	partition int
	metadata  *core.Metadata
	key       bytemap.ByteMap
	vals      core.Vals
	flatRow   *core.FlatRow
	totalRows int
	elapsed   time.Duration
	err       error
}

func (db *DB) queryCluster(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, includeMemStore bool, unflat bool, onMetadata core.OnMetadata, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
	ctx = common.WithIncludeMemStore(ctx, includeMemStore)
	numPartitions := db.opts.NumPartitions
	results := make(chan *remoteResult, numPartitions*100000) // TODO: make this tunable
	resultsByPartition := make(map[int]*int64)
	var _finalErr error
	var finalErrMx sync.RWMutex
	finalErr := func() error {
		finalErrMx.RLock()
		result := _finalErr
		finalErrMx.RUnlock()
		return result
	}
	fail := func(err error) {
		finalErrMx.Lock()
		if _finalErr != nil {
			_finalErr = err
		}
		finalErrMx.Unlock()
	}
	_stopped := int64(0)
	stopped := func() bool {
		return atomic.LoadInt64(&_stopped) == 1
	}
	stop := func() {
		atomic.StoreInt64(&_stopped, 1)
	}

	subCtx := ctx
	ctxDeadline, ctxHasDeadline := subCtx.Deadline()
	if ctxHasDeadline {
		// Halve timeout for sub-contexts
		now := time.Now()
		timeout := ctxDeadline.Sub(now)
		var cancel context.CancelFunc
		ctxDeadline = now.Add(timeout / 2)
		subCtx, cancel = context.WithDeadline(subCtx, ctxDeadline)
		defer cancel()
	}

	for i := 0; i < numPartitions; i++ {
		partition := i
		_resultsForPartition := int64(0)
		resultsForPartition := &_resultsForPartition
		resultsByPartition[partition] = resultsForPartition
		go func() {
			for {
				elapsed := mtime.Stopwatch()
				query := db.remoteQueryHandlerForPartition(partition)
				if query == nil {
					log.Errorf("No query handler for partition %d, ignoring", partition)
					results <- &remoteResult{
						partition: partition,
						totalRows: 0,
						elapsed:   elapsed(),
						err:       nil,
					}
					break
				}

				var partOnRow func(key bytemap.ByteMap, vals core.Vals) (bool, error)
				var partOnFlatRow func(row *core.FlatRow) (bool, error)
				if unflat {
					partOnRow = func(key bytemap.ByteMap, vals core.Vals) (bool, error) {
						err := finalErr()
						if err != nil {
							return false, err
						}
						if stopped() {
							return false, nil
						}
						results <- &remoteResult{
							partition: partition,
							key:       key,
							vals:      vals,
						}
						atomic.AddInt64(resultsForPartition, 1)
						return true, nil
					}
				} else {
					partOnFlatRow = func(row *core.FlatRow) (bool, error) {
						err := finalErr()
						if err != nil {
							return false, err
						}
						if stopped() {
							return false, nil
						}
						results <- &remoteResult{
							partition: partition,
							flatRow:   row,
						}
						atomic.AddInt64(resultsForPartition, 1)
						return true, nil
					}
				}

				err := query(subCtx, sqlString, isSubQuery, subQueryResults, unflat, func(md *core.Metadata) error {
					results <- &remoteResult{
						partition: partition,
						metadata:  md,
					}
					return nil
				}, partOnRow, partOnFlatRow)
				if err != nil && atomic.LoadInt64(resultsForPartition) == 0 {
					log.Debugf("Failed on partition %d, haven't read anything, continuing: %v", partition, err)
					continue
				}
				results <- &remoteResult{
					partition: partition,
					totalRows: int(atomic.LoadInt64(resultsForPartition)),
					elapsed:   elapsed(),
					err:       err,
				}
				break
			}
		}()
	}

	start := time.Now()
	deadline := start.Add(10 * time.Minute)
	if ctxHasDeadline {
		deadline = ctxDeadline
	}
	log.Debugf("Deadline for results from partitions: %v (T - %v)", deadline, deadline.Sub(time.Now()))

	timeout := time.NewTimer(deadline.Sub(time.Now()))
	var canonicalFields core.Fields
	fieldsByPartition := make([]core.Fields, db.opts.NumPartitions)
	partitionRowMappers := make([]func(core.Vals) core.Vals, db.opts.NumPartitions)
	resultCount := 0
	for pendingPartitions := numPartitions; pendingPartitions > 0; {
		select {
		case result := <-results:
			// first handle fields
			partitionFields := result.metadata.Fields
			if partitionFields != nil {
				if canonicalFields == nil {
					log.Debugf("fields: %v", partitionFields)
					err := onMetadata(result.metadata.WithFields(partitionFields))
					if err != nil {
						fail(err)
					}
					canonicalFields = partitionFields
				}

				// Each partition can theoretically have different field definitions.
				// To accomodate this, we track the fields separate for each partition
				// and convert into the canonical form before sending onward.
				fieldsByPartition[result.partition] = partitionFields
				partitionRowMappers[result.partition] = partitionRowMapper(canonicalFields, partitionFields)
				continue
			}

			// handle unflat rows
			if result.key != nil {
				if stopped() || finalErr() != nil {
					continue
				}
				more, err := onRow(result.key, partitionRowMappers[result.partition](result.vals))
				if err != nil {
					fail(err)
				} else if !more {
					stop()
				}
				continue
			}

			// handle flat rows
			flatRow := result.flatRow
			if flatRow != nil {
				if stopped() || finalErr() != nil {
					continue
				}
				flatRow.SetFields(fieldsByPartition[result.partition])
				more, err := onFlatRow(flatRow)
				if err != nil {
					fail(err)
					return err
				} else if !more {
					stop()
				}
				continue
			}

			// final results for partition
			resultCount++
			pendingPartitions--
			if result.err != nil {
				log.Errorf("Error from partition %d: %v", result.partition, result.err)
				fail(result.err)
			}
			log.Debugf("%d/%d got %d results from partition %d in %v", resultCount, db.opts.NumPartitions, result.totalRows, result.partition, result.elapsed)
			delete(resultsByPartition, result.partition)
		case <-timeout.C:
			fail(core.ErrDeadlineExceeded)
			log.Errorf("Failed to get results by deadline, %d of %d partitions reporting", resultCount, numPartitions)
			msg := bytes.NewBuffer([]byte("Missing partitions: "))
			first := true
			for partition, results := range resultsByPartition {
				if !first {
					msg.WriteString(" | ")
				}
				first = false
				msg.WriteString(fmt.Sprintf("%d (%d)", partition, results))
			}
			log.Debug(msg.String())
			return finalErr()
		}
	}

	return finalErr()
}

func partitionRowMapper(canonicalFields core.Fields, partitionFields core.Fields) func(core.Vals) core.Vals {
	if canonicalFields.Equals(partitionFields) {
		return func(vals core.Vals) core.Vals { return vals }
	}

	scratch := make(core.Vals, len(canonicalFields))
	idxs := make([]int, 0, len(canonicalFields))
	for _, canonicalField := range canonicalFields {
		i := -1
		for _i, partitionField := range partitionFields {
			if canonicalField.Equals(partitionField) {
				i = _i
				break
			}
		}
		idxs = append(idxs, i)
	}

	return func(vals core.Vals) core.Vals {
		for o, i := range idxs {
			if i < 0 || i >= len(vals) {
				scratch[o] = nil
			} else {
				scratch[o] = vals[i]
			}
		}

		for i := range vals {
			vals[i] = scratch[i]
		}

		return vals
	}
}
