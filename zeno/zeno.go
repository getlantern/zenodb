package main

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/getlantern/goexpr/isp"
	"github.com/getlantern/goexpr/isp/ip2location"
	"github.com/getlantern/goexpr/isp/maxmind"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/rpc"
)

var (
	log = golog.LoggerFor("zeno")

	dbdir             = flag.String("dbdir", "zenodb", "The directory in which to store the database files, defaults to ./zenodb")
	schema            = flag.String("schema", "schema.yaml", "Location of schema file, defaults to ./schema.yaml")
	ispformat         = flag.String("ispformat", "ip2location", "ip2location or maxmind")
	ispdb             = flag.String("ispdb", "", "In order to enable ISP functions, point this to a ISP database file, either in IP2Location Lite format or MaxMind GeoIP2 ISP format")
	fresh             = flag.Bool("fresh", false, "Set this flag to include data not yet flushed from memstore in query results")
	vtime             = flag.Bool("vtime", false, "Set this flag to use virtual instead of real time. When using virtual time, the advancement of time will be governed by the timestamps received via insterts.")
	walSync           = flag.Duration("walsync", 5*time.Second, "How frequently to sync the WAL to disk. Set to 0 to sync after every write. Defaults to 5 seconds.")
	maxWALAge         = flag.Duration("maxwalage", 336*time.Hour, "Maximum age for WAL files. Files older than this will be deleted. Defaults to 336 hours (2 weeks).")
	walCompressionAge = flag.Duration("walcompressage", 1*time.Hour, "Age at which to start compressing WAL files with gzip. Defaults to 1 hour.")
	addr              = flag.String("addr", "localhost:17712", "The address at which to listen for gRPC connections, defaults to localhost:17712")
	httpAddr          = flag.String("http-addr", "localhost:17713", "The address at which to listen for JSON over HTTP connections, defaults to localhost:17713")
	pprofAddr         = flag.String("pprofaddr", "localhost:4000", "if specified, will listen for pprof connections at the specified tcp address")
	password          = flag.String("password", "", "if specified, will authenticate clients using this password")
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

	var ispProvider isp.Provider
	var providerErr error
	if *ispformat != "" && *ispdb != "" {
		log.Debugf("Enabling ISP functions using format %v with db file at %v", *ispformat, *ispdb)

		switch strings.ToLower(strings.TrimSpace(*ispformat)) {
		case "ip2location":
			ispProvider, providerErr = ip2location.NewProvider(*ispdb)
		case "maxmind":
			ispProvider, providerErr = maxmind.NewProvider(*ispdb)
		default:
			log.Errorf("Unknown ispdb format %v", *ispformat)
		}
		if providerErr != nil {
			log.Errorf("Unable to initialize ISP provider %v from %v: %v", *ispformat, *ispdb, err)
			ispProvider = nil
		}
	}

	db, err := zenodb.NewDB(&zenodb.DBOpts{
		Dir:                    *dbdir,
		SchemaFile:             *schema,
		ISPProvider:            ispProvider,
		IncludeMemStoreInQuery: *fresh,
		VirtualTime:            *vtime,
		WALSyncInterval:        *walSync,
		MaxWALAge:              *maxWALAge,
		WALCompressionAge:      *walCompressionAge,
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
	err := rpc.Serve(db, l, &rpc.ServerOpts{
		Password: *password,
	})
	if err != nil {
		log.Fatalf("Error serving gRPC: %v", err)
	}
}
