package web

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/gorilla/mux"
	"github.com/retailnext/hllpp"
)

const (
	nanosPerMilli = 1000000
)

type QueryResult struct {
	SQL                string
	Permalink          string
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

	for {
		ce, err := h.query(req, sqlString)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(resp, err.Error())
			return
		}
		switch ce.status() {
		case statusSuccess:
			h.respondSuccess(resp, req, ce)
			return
		case statusError:
			h.respondError(resp, req, ce)
			return
		case statusPending:
			// Wait and then continue
			time.Sleep(1 * time.Second)
		}
	}
}

func (h *handler) asyncQuery(resp http.ResponseWriter, req *http.Request) {
	if !h.authenticate(resp, req) {
		resp.WriteHeader(http.StatusForbidden)
		return
	}

	sqlString, _ := url.QueryUnescape(req.URL.RawQuery)

	ce, err := h.query(req, sqlString)
	h.respondWithCacheEntry(resp, req, ce, err, 0)
}

func (h *handler) cachedQuery(resp http.ResponseWriter, req *http.Request) {
	if !h.authenticate(resp, req) {
		resp.WriteHeader(http.StatusForbidden)
		return
	}

	permalink := mux.Vars(req)["permalink"]
	ce, err := h.cache.getByPermalink(permalink)
	if ce == nil {
		http.NotFound(resp, req)
		return
	}
	h.respondWithCacheEntry(resp, req, ce, err, 1*time.Second)
}

func (h *handler) respondWithCacheEntry(resp http.ResponseWriter, req *http.Request, ce cacheEntry, err error, waitTime time.Duration) {
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(resp, err.Error())
		return
	}
	switch ce.status() {
	case statusSuccess:
		h.respondSuccess(resp, req, ce)
	case statusError:
		h.respondError(resp, req, ce)
	case statusPending:
		// Wait a little bit
		time.Sleep(waitTime)
		// Let the client know that we're still working on it
		http.Redirect(resp, req, fmt.Sprintf("/cached/%v", ce.permalink()), http.StatusFound)
	}
}

func (h *handler) respondSuccess(resp http.ResponseWriter, req *http.Request, ce cacheEntry) {
	resp.Header().Set("Content-Type", "application/json")
	resp.Header().Set("Cache-control", fmt.Sprintf("max-age=%d", int64(h.CacheTTL.Seconds())))
	resp.WriteHeader(http.StatusOK)
	resp.Write(ce.data())
}

func (h *handler) respondError(resp http.ResponseWriter, req *http.Request, ce cacheEntry) {
	resp.WriteHeader(http.StatusInternalServerError)
	resp.Write(ce.error())
}

func (h *handler) query(req *http.Request, sqlString string) (ce cacheEntry, err error) {
	if req.Header.Get("Cache-control") == "no-cache" {
		ce, err = h.cache.begin(sqlString)
		if err != nil {
			return
		}
	} else {
		var created bool
		ce, created, err = h.cache.getOrBegin(sqlString)
		if err != nil || !created {
			return
		}
		if ce.status() != statusPending {
			log.Debugf("Found results for %v in cache", sqlString)
			return
		}
	}

	// Run the query in the background
	go func() {
		var result *QueryResult
		result, err = h.doQuery(sqlString, ce.permalink())
		if err != nil {
			err = fmt.Errorf("Unable to query: %v", err)
			log.Error(err)
			ce = ce.fail(err)
		} else {
			resultBytes, err := json.Marshal(result)
			if err != nil {
				ce = ce.fail(fmt.Errorf("Unable to marshal result: %v", err))
			} else {
				ce = ce.succeed(resultBytes)
			}
		}
		h.cache.put(sqlString, ce)
	}()

	return
}

func (h *handler) doQuery(sqlString string, permalink string) (*QueryResult, error) {
	rs, err := h.db.Query(sqlString, false, nil, false)
	if err != nil {
		return nil, err
	}

	var addDim func(dim string)

	result := &QueryResult{
		SQL:       sqlString,
		Permalink: permalink,
		TS:        time.Now().UnixNano() / nanosPerMilli,
	}
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

	var fields core.Fields
	var fieldCardinalities []*hllpp.HLLPP
	dimCardinalities := make(map[string]*hllpp.HLLPP)
	tsCardinality := hllpp.New()
	cbytes := make([]byte, 8)

	var mx sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rs.Iterate(ctx, func(inFields core.Fields) error {
		fields = inFields
		for _, field := range fields {
			result.Fields = append(result.Fields, field.Name)
			fieldCardinalities = append(fieldCardinalities, hllpp.New())
		}
		return nil
	}, func(row *core.FlatRow) (bool, error) {
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

		encoding.Binary.PutUint64(cbytes, uint64(row.TS))
		tsCardinality.Add(cbytes)

		resultRow := &ResultRow{
			TS:   row.TS / nanosPerMilli,
			Key:  key,
			Vals: make([]float64, 0, len(row.Values)),
		}

		for i, value := range row.Values {
			resultRow.Vals = append(resultRow.Vals, value)
			encoding.Binary.PutUint64(cbytes, math.Float64bits(value))
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
	encoding.Binary.PutUint64(b, i)
	return b
}
