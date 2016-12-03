// Package queryplanner provides functionality for planning the execution of
// queries.
package queryplanner

import (
	"errors"
	"github.com/getlantern/zenodb/pipeline"
	"github.com/getlantern/zenodb/sql"
)

var (
	ErrNestedFromSubquery = errors.New("nested FROM subqueries not supported")
)

type Opts struct {
	FieldSource   sql.FieldSource
	Distributed   bool
	PartitionKeys []string
}

func Plan(sqlString string, opts *Opts) (QueryPlan, error) {
	query, err := sql.Parse(sqlString, opts.FieldSource)
	if err != nil {
		return nil, err
	}

	if query.FromSubQuery != nil && query.FromSubQuery.FromSubQuery != nil {
		return nil, ErrNestedFromSubquery
	}

	if opts.Distributed {
		return planDistributed(query, opts)
	}

	return planPlain(query, opts)
}
