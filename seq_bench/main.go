package main

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/getlantern/tdb"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:4000", nil)
	}()

	tdb.BenchmarkSeq()
}
