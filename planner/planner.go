// Package planner provides functionality for planning the execution of queries.
package planner

import (
	"errors"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/sql"
)

var (
	ErrNestedFromSubquery = errors.New("nested FROM subqueries not supported")
)

type Opts struct {
	GetTable      func(name string) core.Source
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
