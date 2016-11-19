package zenodb

import (
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/zenodb/bytetree"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/sql"
)

type subqueryResult struct {
	bt          *bytetree.Tree
	qr          *QueryResult
	knownFields []sql.Field
}

func (exec *queryExecution) runSubQuery() (queryable, error) {
	subQuery := &Query{db: exec.db, Query: *exec.FromSubQuery}
	// TODO: there's probably a more efficient way to get a queryable
	result, err := subQuery.Run(false)
	if err != nil {
		return nil, err
	}
	return exec.newSubqueryResult(result), nil
}

func (exec *queryExecution) newSubqueryResult(qr *QueryResult) *subqueryResult {
	bt := bytetree.New()
	for _, row := range qr.Rows {
		ts := qr.exec.q.until.Add(-1 * time.Duration(row.Period) * qr.exec.Resolution)
		key := bytemap.FromSortedKeysAndValues(qr.GroupBy, row.Dims)
		vals := encoding.NewTSParams(ts, bytemap.FromSortedKeysAndFloats(qr.FieldNames, row.Values))
		bt.Update(exec.Fields, qr.exec.Resolution, qr.exec.q.asOf, key, vals, key)
	}
	return &subqueryResult{
		bt:          bt,
		qr:          qr,
		knownFields: exec.Fields,
	}
}

func (sr *subqueryResult) fields() []sql.Field {
	return sr.knownFields
}

func (sr *subqueryResult) resolution() time.Duration {
	return sr.qr.exec.Resolution
}

func (sr *subqueryResult) retentionPeriod() time.Duration {
	retentionPeriod := sr.qr.exec.q.until.Sub(sr.qr.exec.q.asOf)
	resolution := sr.resolution()
	if retentionPeriod < resolution {
		retentionPeriod = resolution
	}
	return retentionPeriod
}

func (sr *subqueryResult) truncateBefore() time.Time {
	return sr.qr.exec.q.asOf
}

func (sr *subqueryResult) iterate(fields []string, includeMemStore bool, onValue func(bytemap.ByteMap, []encoding.Sequence)) error {
	sr.bt.Walk(0, func(key []byte, columns []encoding.Sequence) bool {
		onValue(bytemap.ByteMap(key), columns)
		return false
	})
	return nil
}
