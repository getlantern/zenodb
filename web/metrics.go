package web

import (
	"encoding/json"
	"net/http"

	"github.com/getlantern/zenodb/metrics"
)

func (h *handler) metrics(resp http.ResponseWriter, req *http.Request) {
	if !h.authenticate(resp, req) {
		resp.WriteHeader(http.StatusForbidden)
		return
	}

	json.NewEncoder(resp).Encode(metrics.GetStats())
}
