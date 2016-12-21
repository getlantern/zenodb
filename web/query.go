package web

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/getlantern/zenodb/core"
	"net/http"
	"path"
	"sort"
	"sync"
	"time"
)

type QueryResult struct {
	Fields []string
	Dims   []string
	Rows   []*ResultRow
}

type ResultRow struct {
	TS   int64
	Key  map[string]interface{}
	Vals []float64
}

func (h *handler) runQuery(resp http.ResponseWriter, req *http.Request) {
	if !h.authenticate(resp, req) {
		return
	}

	_, sqlString := path.Split(req.URL.Path)
	rs, err := h.db.Query(sqlString, false, nil, false)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(resp, "Unable to query: %v", err)
		return
	}

	var addDim func(dim string)

	result := &QueryResult{}
	groupBy := rs.GetGroupBy()
	if len(groupBy) > 0 {
		addDim = func(dim string) {
			// noop
		}
		for _, gb := range groupBy {
			result.Dims = append(result.Dims, gb.Name)
		}
	} else {
		addDim = func(dim string) {
			found := false
			for _, existing := range result.Dims {
				if existing == dim {
					found = true
					break
				}
			}
			if !found {
				result.Dims = append(result.Dims, dim)
			}
		}
	}

	for _, field := range rs.GetFields() {
		result.Fields = append(result.Fields, field.Name)
	}

	var mx sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rs.Iterate(ctx, func(row *core.FlatRow) (bool, error) {
		mx.Lock()
		key := row.Key.AsMap()
		for dim := range key {
			addDim(dim)
		}
		resultRow := &ResultRow{
			TS:   row.TS,
			Key:  key,
			Vals: make([]float64, 0, len(row.Values)),
		}
		for _, value := range row.Values {
			resultRow.Vals = append(resultRow.Vals, value)
		}
		result.Rows = append(result.Rows, resultRow)
		mx.Unlock()
		return true, nil
	})

	sort.Strings(result.Dims)
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(resp)
	enc.Encode(result)
}
