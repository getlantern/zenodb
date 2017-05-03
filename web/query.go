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

	"github.com/dustin/go-humanize"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/gorilla/mux"
	"github.com/retailnext/hllpp"
)

const (
	nanosPerMilli = 1000000

	pauseTime    = 250 * time.Millisecond
	shortTimeout = 5 * time.Second
	longTimeout  = 1000 * time.Hour
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
	h.sqlQuery(resp, req, longTimeout)
}

func (h *handler) asyncQuery(resp http.ResponseWriter, req *http.Request) {
	h.sqlQuery(resp, req, shortTimeout)
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
	h.respondWithCacheEntry(resp, req, ce, err, shortTimeout)
}

func (h *handler) sqlQuery(resp http.ResponseWriter, req *http.Request, timeout time.Duration) {
	if !h.authenticate(resp, req) {
		resp.WriteHeader(http.StatusForbidden)
		return
	}

	sqlString, _ := url.QueryUnescape(req.URL.RawQuery)

	ce, err := h.query(req, sqlString)
	h.respondWithCacheEntry(resp, req, ce, err, timeout)
}

func (h *handler) respondWithCacheEntry(resp http.ResponseWriter, req *http.Request, ce cacheEntry, err error, timeout time.Duration) {
	limit := int(timeout / pauseTime)
	for i := 0; i < limit; i++ {
		if err != nil {
			log.Error(err)
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
			// Pause a little bit and try again
			time.Sleep(pauseTime)
			ce, err = h.cache.getByPermalink(ce.permalink())
		}
	}
	// Let the client know that we're still working on it
	resp.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(resp, "/cached/%v", ce.permalink())
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
				err = fmt.Errorf("Unable to marshal result: %v", err)
				log.Error(err)
				ce = ce.fail(err)
			} else if len(resultBytes) > h.MaxResponseBytes {
				err = fmt.Errorf("Query result size %v exceeded limit of %v", humanize.Bytes(uint64(len(resultBytes))), humanize.Bytes(uint64(h.MaxResponseBytes)))
				log.Error(err)
				ce = ce.fail(err)
			} else {
				ce = ce.succeed(resultBytes)
			}
		}
		h.cache.put(sqlString, ce)
		log.Debugf("Cached results for %v", sqlString)
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

	estimatedResultBytes := 0
	var mx sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), h.QueryTimeout)
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
			estimatedResultBytes += len(dim) + len(valueBytes)
			return true
		})

		estimatedResultBytes += 8 * len(row.Values)
		if estimatedResultBytes > h.MaxResponseBytes {
			mx.Unlock()
			// Note - the estimated size here is always an underestimate of the final
			// JSON size, so this is a conservative way to check. The final check
			// after generating the JSON may sometimes catch things that slipped
			// through here.
			return false, fmt.Errorf("Estimated query result size %v exceeded limit of %v", humanize.Bytes(uint64(estimatedResultBytes)), humanize.Bytes(uint64(h.MaxResponseBytes)))
		}

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
