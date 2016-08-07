package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/getlantern/zenodb"
	"github.com/gorilla/mux"
)

const (
	// ContentType is the key for the Content-Type header
	ContentType = "Content-Type"

	// ContentTypeJSON is the allowed content type
	ContentTypeJSON = "application/json"
)

func serveHTTP(db *zenodb.DB, hl net.Listener) {
	r := mux.NewRouter()
	r.HandleFunc("/insert/{stream}", httpHandler(db))

	s := &http.Server{
		Handler: r,
	}

	err := s.Serve(hl)
	if err != nil {
		log.Fatalf("Error serving HTTP: %v", err)
	}
}

func httpHandler(db *zenodb.DB) func(resp http.ResponseWriter, req *http.Request) {
	return func(resp http.ResponseWriter, req *http.Request) {
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
			point := &zenodb.Point{}
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
			if point.Ts.IsZero() {
				point.Ts = time.Now()
			}
			if point.Dims == nil || len(point.Dims) == 0 {
				badRequest(resp, "Need at least one dim")
				return
			}
			if point.Vals == nil || len(point.Vals) == 0 {
				badRequest(resp, "Need at least one val")
				return
			}

			insertErr := db.Insert(stream, point)
			if insertErr != nil {
				internalServerError(resp, "Error submitting point: %v", err)
			}
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
