package main

import (
	"flag"
	"fmt"
	"net"

	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/rpc"
)

var (
	log = golog.LoggerFor("zeno")

	dbdir     = flag.String("dbdir", "zenodb", "The directory in which to store the database files, defaults to ./zenodb")
	schema    = flag.String("schema", "schema.yaml", "Location of schema file, defaults to ./schema.yaml")
	ispdb     = flag.String("ispdb", "", "In order to enable ISP functions, point this to an IP2Location Lite ISP database file like the one here - https://lite.ip2location.com/database/ip-asn")
	fresh     = flag.Bool("fresh", false, "Set this flag to include data not yet flushed from memstore in query results")
	vtime     = flag.Bool("vtime", false, "Set this flag to use virtual instead of real time.  When using virtual time, the advancement of time will be governed by the timestamps received via insterts.")
	addr      = flag.String("addr", "localhost:17712", "The address at which to listen for gRPC connections, defaults to localhost:17712")
	httpAddr  = flag.String("http-addr", "localhost:17713", "The address at which to listen for JSON over HTTP connections, defaults to localhost:17713")
	pprofAddr = flag.String("pprofaddr", "localhost:4000", "if specified, will listen for pprof connections at the specified tcp address")
)

func main() {
	flag.Parse()

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Unable to listen for gRPC connections at %v: %v", *addr, err)
	}

	hl, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		log.Fatalf("Unable to listen for HTTP connections at %v: %v", *httpAddr, err)
	}

	db, err := zenodb.NewDB(&zenodb.DBOpts{
		Dir:                    *dbdir,
		SchemaFile:             *schema,
		ISPDatabase:            *ispdb,
		IncludeMemStoreInQuery: *fresh,
		VirtualTime:            *vtime,
	})

	if err != nil {
		log.Fatalf("Unable to open database at %v: %v", *dbdir, err)
	}
	fmt.Printf("Opened database at %v\n", *dbdir)

	fmt.Printf("Listening for gRPC connections at %v\n", l.Addr())
	fmt.Printf("Listening for HTTP connections at %v\n", hl.Addr())

	go serveHTTP(db, hl)
	serveRPC(db, l)
}

func serveRPC(db *zenodb.DB, l net.Listener) {
	err := rpc.Serve(db, l)
	if err != nil {
		log.Fatalf("Error serving gRPC: %v", err)
	}
}
