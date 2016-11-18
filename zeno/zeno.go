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
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/rpc"
	"golang.org/x/net/context"
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
	httpAddr          = flag.String("httpaddr", "localhost:17713", "The address at which to listen for JSON over HTTP connections, defaults to localhost:17713")
	pprofAddr         = flag.String("pprofaddr", "localhost:4000", "if specified, will listen for pprof connections at the specified tcp address")
	password          = flag.String("password", "", "if specified, will authenticate clients using this password")
	passthrough       = flag.Bool("passthrough", false, "set to true to make this node a passthrough that doesn't capture data in table but is capable of feeding and querying other nodes. requires that -partitions to be specified.")
	capture           = flag.String("capture", "", "if specified, connect to the node at the given address to receive updates, authenticating with value of -password.  requires that you specify which -partition this node handles.")
	feed              = flag.String("feed", "", "if specified, connect to the nodes at the given comma,delimited addresses to handle queries for them, authenticating with value of -password. requires that you specify which -partition this node handles.")
	numPartitions     = flag.Int("numpartitions", 1, "The number of partitions available to distribute amongst followers")
	partition         = flag.Int("partition", 0, "use with -follow, the partition number assigned to this follower")
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

	var follow func(f *zenodb.Follow, cb func(data []byte, newOffset wal.Offset) error)
	var registerQueryHandler func(partition int, query zenodb.QueryFN)
	if *capture != "" {
		client, dialErr := rpc.Dial(*capture, &rpc.ClientOpts{
			Password: *password,
		})
		if dialErr != nil {
			log.Fatalf("Unable to connect to passthrough at %v: %v", *capture, dialErr)
		}
		log.Debugf("Capturing data from %v", *capture)
		follow = func(f *zenodb.Follow, cb func(data []byte, newOffset wal.Offset) error) {
			followFunc, followErr := client.Follow(context.Background(), f)
			if followErr != nil {
				log.Fatalf("Error following stream %v: %v", f.Stream, followErr)
			}
			for {
				data, newOffset, followErr := followFunc()
				if followErr != nil {
					log.Fatalf("Error reading from stream %v: %v", f.Stream, followErr)
				}
				log.Tracef("Got data for %v", f.Stream)
				followErr = cb(data, newOffset)
				if err != nil {
					log.Fatalf("Error reading from stream %v: %v", f.Stream, followErr)
				}
			}
		}
	}
	if *feed != "" {
		leaders := strings.Split(*feed, ",")
		clients := make([]rpc.Client, 0, len(leaders))
		for _, leader := range leaders {
			client, dialErr := rpc.Dial(leader, &rpc.ClientOpts{
				Password: *password,
			})
			if dialErr != nil {
				log.Fatalf("Unable to connect to query leader at %v: %v", leader, dialErr)
			}
			clients = append(clients, client)
			log.Debugf("Handling queries for: %v", leader)
		}
		registerQueryHandler = func(partition int, query zenodb.QueryFN) {
			minWaitTime := 50 * time.Millisecond
			maxWaitTime := 5 * time.Second

			for i := 0; i < len(leaders); i++ {
				client := clients[i]
				// TODO: make query concurrency configurable
				for j := 0; j < 8; j++ {
					go func() {
						// Continually handle queries and then reconnect for next query
						waitTime := minWaitTime
						for {
							handleErr := client.ProcessRemoteQuery(context.Background(), partition, query)
							if handleErr == nil {
								waitTime = minWaitTime
							} else {
								log.Errorf("Error handling queries: %v", handleErr)
								// Exponential back-off
								time.Sleep(waitTime)
								waitTime *= 2
								if waitTime > maxWaitTime {
									waitTime = maxWaitTime
								}
							}
						}
					}()
				}
			}
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
		Passthrough:            *passthrough,
		NumPartitions:          *numPartitions,
		Partition:              *partition,
		Follow:                 follow,
		RegisterRemoteQueryHandler: registerQueryHandler,
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
