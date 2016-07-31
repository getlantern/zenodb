package main

import (
	"flag"
	"fmt"
	"net"
	"time"

	"github.com/getlantern/golog"
	"github.com/getlantern/tibsdb"
	"github.com/getlantern/tibsdb/rpc"
)

var (
	log = golog.LoggerFor("tibsdb")

	dbdir     = flag.String("dbdir", "tibsdb", "The directory in which to store the database files, defaults to ./tibsdb")
	addr      = flag.String("addr", "localhost:17712", "The address at which to listen for gRPC connections, defaults to localhost:17712")
	pprofAddr = flag.String("pprofaddr", "localhost:4000", "if specified, will listen for pprof connections at the specified tcp address")
)

func main() {
	flag.Parse()

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Unable to listen at %v: %v", *addr, err)
	}

	db, err := tibsdb.NewDB(&tibsdb.DBOpts{
		Dir: *dbdir,
	})
	if err != nil {
		log.Fatalf("Unable to open database at %v: %v", *dbdir, err)
	}
	fmt.Printf("Opened database at %v\n", *dbdir)

	err = db.CreateTable(&tibsdb.TableOpts{
		Name:             "test",
		RetentionPeriod:  120 * time.Minute,
		MaxMemStoreBytes: 500 * 1024 * 1024,
		MaxFlushLatency:  5 * time.Minute,
		SQL: fmt.Sprintf(`
SELECT
	SUM(i) AS i,
	SUM(ii) AS ii,
	AVG(ii) / AVG(i) AS iii
FROM inbound
GROUP BY period(%v)`, 5*time.Minute),
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Listening for connections at %v\n", l.Addr())
	serverErr := rpc.Serve(db, l)
	if err != nil {
		log.Fatal(serverErr)
	}
}
