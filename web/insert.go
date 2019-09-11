package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

const (
	// ContentType is the key for the Content-Type header
	ContentType = "Content-Type"

	// ContentTypeJSON is the allowed content type
	ContentTypeJSON = "application/json"
)

type Point struct {
	Ts   time.Time              `json:"ts,omitempty"`
	Dims map[string]interface{} `json:"dims,omitempty"`
	Vals map[string]interface{} `json:"vals,omitempty"`
}

func (h *handler) insert(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(resp, "Method %v not allowed\n", req.Method)
		return
	}

	contentType := req.Header.Get(ContentType)
	if contentType != ContentTypeJSON {
		resp.WriteHeader(http.StatusUnsupportedMediaType)
		fmt.Fprintf(resp, "Media type %v unsupported\n", contentType)
		return
	}

	stream := mux.Vars(req)["stream"]
	dec := json.NewDecoder(req.Body)
	for {
		point := &Point{}
		err := dec.Decode(point)
		if err == io.EOF {
			// Done reading points
			resp.WriteHeader(http.StatusCreated)
			return
		}
		if err != nil {
			badRequest(resp, "Error decoding JSON: %v", err)
			return
		}
		if len(point.Dims) == 0 {
			badRequest(resp, "Need at least one dim")
			return
		}
		if len(point.Vals) == 0 {
			badRequest(resp, "Need at least one val")
			return
		}
		if point.Ts.IsZero() {
			point.Ts = time.Now()
		}

		insertErr := h.db.Insert(stream, point.Ts, point.Dims, point.Vals)
		if insertErr != nil {
			internalServerError(resp, "Error submitting point: %v", insertErr)
		}
	}
}

func badRequest(resp http.ResponseWriter, msg string, args ...interface{}) {
	resp.WriteHeader(http.StatusBadRequest)
	log.Errorf(msg, args...)
	fmt.Fprintf(resp, msg+"\n", args...)
}

func internalServerError(resp http.ResponseWriter, msg string, args ...interface{}) {
	resp.WriteHeader(http.StatusInternalServerError)
	log.Errorf(msg, args...)
	fmt.Fprintf(resp, msg+"\n", args...)
}
