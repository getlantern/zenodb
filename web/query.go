package web

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/getlantern/zenodb/core"
	"github.com/retailnext/hllpp"
	"math"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
)

const (
	nanosPerMilli = 1000000
)

type QueryResult struct {
	TS                 int64
	TSCardinality      uint64
	Fields             []string
	FieldCardinalities []uint64
	Dims               []string
	DimCardinalities   []uint64
	Rows               []*ResultRow
}

type ResultRow struct {
	TS   int64
	Key  map[string]interface{}
	Vals []float64
}

func (h *handler) runQuery(resp http.ResponseWriter, req *http.Request) {
	if !h.authenticate(resp, req) {
		resp.WriteHeader(http.StatusForbidden)
		return
	}

	sqlString, _ := url.QueryUnescape(req.URL.RawQuery)

	var resultBytes []byte
	if req.Header.Get("Cache-control") != "no-cache" {
		resultBytes = h.cache.get(sqlString)
	}
	if resultBytes != nil {
		log.Debugf("Found results for %v in cache", sqlString)
	} else {
		result, err := h.doQuery(sqlString)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(resp, "Unable to query: %v", err)
			return
		}
		resultBytes, err = json.Marshal(result)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(resp, "Unable to marshal result: %v", err)
			return
		}
		if len(resultBytes) < h.MaxCacheEntryBytes {
			log.Debugf("Caching result for %v", sqlString)
			h.cache.put(sqlString, resultBytes)
		}
	}

	resp.Header().Set("Content-Type", "application/json")
	resp.Header().Set("Cache-control", fmt.Sprintf("max-age=%d", int64(h.CacheTTL.Seconds())))
	resp.WriteHeader(http.StatusOK)
	resp.Write(resultBytes)
}

func (h *handler) doQuery(sqlString string) (*QueryResult, error) {
	rs, err := h.db.Query(sqlString, false, nil, false)
	if err != nil {
		return nil, err
	}

	var addDim func(dim string)

	result := &QueryResult{TS: time.Now().UnixNano() / nanosPerMilli}
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

	fields := rs.GetFields()
	fieldCardinalities := make([]*hllpp.HLLPP, 0, len(fields))
	for _, field := range fields {
		result.Fields = append(result.Fields, field.Name)
		fieldCardinalities = append(fieldCardinalities, hllpp.New())
	}
	dimCardinalities := make(map[string]*hllpp.HLLPP)
	tsCardinality := hllpp.New()
	cbytes := make([]byte, 8)

	var mx sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rs.Iterate(ctx, func(row *core.FlatRow) (bool, error) {
		mx.Lock()
		key := make(map[string]interface{}, 10)
		row.Key.Iterate(true, true, func(dim string, value interface{}, valueBytes []byte) bool {
			key[dim] = value
			addDim(dim)
			hlp := dimCardinalities[dim]
			if hlp == nil {
				hlp = hllpp.New()
				dimCardinalities[dim] = hlp
			}
			hlp.Add(valueBytes)
			return true
		})

		binary.BigEndian.PutUint64(cbytes, uint64(row.TS))
		tsCardinality.Add(cbytes)

		resultRow := &ResultRow{
			TS:   row.TS / nanosPerMilli,
			Key:  key,
			Vals: make([]float64, 0, len(row.Values)),
		}

		for i, value := range row.Values {
			resultRow.Vals = append(resultRow.Vals, value)
			binary.BigEndian.PutUint64(cbytes, math.Float64bits(value))
			fieldCardinalities[i].Add(cbytes)
		}
		result.Rows = append(result.Rows, resultRow)
		mx.Unlock()
		return true, nil
	})

	result.TSCardinality = tsCardinality.Count()
	result.Dims = make([]string, 0, len(dimCardinalities))
	for dim := range dimCardinalities {
		result.Dims = append(result.Dims, dim)
	}
	sort.Strings(result.Dims)
	for _, dim := range result.Dims {
		result.DimCardinalities = append(result.DimCardinalities, dimCardinalities[dim].Count())
	}

	result.FieldCardinalities = make([]uint64, 0, len(fieldCardinalities))
	for _, fieldCardinality := range fieldCardinalities {
		result.FieldCardinalities = append(result.FieldCardinalities, fieldCardinality.Count())
	}

	return result, nil
}

func intToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}
