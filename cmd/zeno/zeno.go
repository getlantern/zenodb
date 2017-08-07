// zeno is the executable for the ZenoDB database, and can run as a standalone
// server, as a cluster leader or as a cluster follower.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/getlantern/golog"
	"github.com/getlantern/tlsdefaults"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/cmd"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/planner"
	"github.com/getlantern/zenodb/rpc"
	"github.com/getlantern/zenodb/rpc/server"
	"github.com/getlantern/zenodb/web"
	"github.com/gorilla/mux"
	"github.com/vharitonsky/iniflags"
	"golang.org/x/net/context"
)

var (
	log = golog.LoggerFor("zeno")

	dbdir              = flag.String("dbdir", "zenodata", "The directory in which to store the database files, defaults to ./zenodata")
	vtime              = flag.Bool("vtime", false, "Set this flag to use virtual instead of real time. When using virtual time, the advancement of time will be governed by the timestamps received via inserts.")
	walSync            = flag.Duration("walsync", 5*time.Second, "How frequently to sync the WAL to disk. Set to 0 to sync after every write. Defaults to 5 seconds.")
	maxWALSize         = flag.Int("maxwalsize", 1024*1024*1024, "Maximum size of WAL segments on disk. Defaults to 1 GB.")
	walCompressionSize = flag.Int("walcompressionsize", 30*1024*1024, "Size above which to start compressing WAL segments with snappy. Defaults to 30 MB.")
	maxMemory          = flag.Float64("maxmemory", 0.7, "Set to a non-zero value to cap the total size of the process as a percentage of total system memory. Defaults to 0.7 = 70%.")
	addr               = flag.String("addr", "localhost:17712", "The address at which to listen for gRPC over TLS connections, defaults to localhost:17712")
	httpsAddr          = flag.String("httpsaddr", "localhost:17713", "The address at which to listen for JSON over HTTPS connections, defaults to localhost:17713")
	password           = flag.String("password", "", "if specified, will authenticate clients using this password")
	pkfile             = flag.String("pkfile", "pk.pem", "path to the private key PEM file")
	certfile           = flag.String("certfile", "cert.pem", "path to the certificate PEM file")
	cookieHashKey      = flag.String("cookiehashkey", "", "key to use for HMAC authentication of web auth cookies, should be 64 bytes, defaults to random 64 bytes if not specified")
	cookieBlockKey     = flag.String("cookieblockkey", "", "key to use for encrypting web auth cookies, should be 32 bytes, defaults to random 32 bytes if not specified")
	oauthClientID      = flag.String("oauthclientid", "", "id to use for oauth client to connect to GitHub")
	oauthClientSecret  = flag.String("oauthclientsecret", "", "secret id to use for oauth client to connect to GitHub")
	gitHubOrg          = flag.String("githuborg", "", "the GitHug org against which web users are authenticated")
	insecure           = flag.Bool("insecure", false, "set to true to disable TLS certificate verification when connecting to other zeno servers (don't use this in production!)")
	passthrough        = flag.Bool("passthrough", false, "set to true to make this node a passthrough that doesn't capture data in table but is capable of feeding and querying other nodes. requires that -partitions to be specified.")
	capture            = flag.String("capture", "", "if specified, connect to the node at the given address to receive updates, authenticating with value of -password.  requires that you specify which -partition this node handles.")
	captureOverride    = flag.String("captureoverride", "", "if specified, dial network connection for -capture using this address, but verify TLS connection using the address from -capture")
	feed               = flag.String("feed", "", "if specified, connect to the nodes at the given comma,delimited addresses to handle queries for them, authenticating with value of -password. requires that you specify which -partition this node handles.")
	feedOverride       = flag.String("feedoverride", "", "if specified, dial network connection for -feed using this address, but verify TLS connection using the address from -feed")
	numPartitions      = flag.Int("numpartitions", 1, "The number of partitions available to distribute amongst followers")
	partition          = flag.Int("partition", 0, "use with -follow, the partition number assigned to this follower")
	maxFollowAge       = flag.Duration("maxfollowage", 0, "user with -follow, limits how far to go back when pulling data from leader")
)

func main() {
	iniflags.Parse()

	cmd.StartPprof()

	l, err := tlsdefaults.Listen(*addr, *pkfile, *certfile)
	if err != nil {
		log.Fatalf("Unable to listen for gRPC over TLS connections at %v: %v", *addr, err)
	}

	hl, err := tlsdefaults.Listen(*httpsAddr, *pkfile, *certfile)
	if err != nil {
		log.Fatalf("Unable to listen for HTTPS connections at %v: %v", *httpsAddr, err)
	}

	clientSessionCache := tls.NewLRUClientSessionCache(10000)
	var follow func(f func() *common.Follow, cb func(data []byte, newOffset wal.Offset) error)
	var registerQueryHandler func(partition int, query planner.QueryClusterFN)
	if *capture != "" {
		host, _, _ := net.SplitHostPort(*capture)
		clientTLSConfig := &tls.Config{
			ServerName:         host,
			InsecureSkipVerify: *insecure,
			ClientSessionCache: clientSessionCache,
		}

		dest := *capture
		if *captureOverride != "" {
			dest = *captureOverride
		}

		clientOpts := &rpc.ClientOpts{
			Password: *password,
			Dialer: func(addr string, timeout time.Duration) (net.Conn, error) {
				conn, dialErr := net.DialTimeout("tcp", dest, timeout)
				if dialErr != nil {
					return nil, dialErr
				}
				tlsConn := tls.Client(conn, clientTLSConfig)
				return tlsConn, tlsConn.Handshake()
			},
		}

		client, dialErr := rpc.Dial(*capture, clientOpts)
		if dialErr != nil {
			log.Fatalf("Unable to connect to passthrough at %v: %v", *capture, dialErr)
		}

		log.Debugf("Capturing data from %v", *capture)
		follow = func(ff func() *common.Follow, insert func(data []byte, newOffset wal.Offset) error) {
			minWait := 1 * time.Second
			maxWait := 1 * time.Minute
			wait := minWait
			for {
				for {
					f := ff()
					followFunc, followErr := client.Follow(context.Background(), f)
					if followErr != nil {
						log.Errorf("Error following stream %v: %v", f.Stream, followErr)
						break
					}
					for {
						data, newOffset, followErr := followFunc()
						if followErr != nil {
							log.Errorf("Error reading from stream %v: %v", f.Stream, followErr)
							break
						}
						insertErr := insert(data, newOffset)
						if insertErr != nil {
							log.Errorf("Error inserting data for stream %v: %v", f.Stream, insertErr)
							break
						}
						f.EarliestOffset = newOffset
						// reset wait time
						wait = minWait
					}
					// exponentialBackoff
					time.Sleep(wait)
					wait *= 2
					if wait > maxWait {
						wait = maxWait
					}
				}
			}
		}
	}

	if *feed != "" {
		leaders := strings.Split(*feed, ",")
		leaderOverrides := strings.Split(*feedOverride, ",")
		if *feedOverride != "" && len(leaders) != len(leaderOverrides) {
			log.Fatal("Number of servers specified to -feed must match -feedOverrides")
		}
		clients := make([]rpc.Client, 0, len(leaders))
		for i, leader := range leaders {
			host, _, _ := net.SplitHostPort(leader)
			clientTLSConfig := &tls.Config{
				ServerName:         host,
				InsecureSkipVerify: *insecure,
				ClientSessionCache: clientSessionCache,
			}

			dest := leader
			if *feedOverride != "" {
				dest = leaderOverrides[i]
			}

			clientOpts := &rpc.ClientOpts{
				Password: *password,
				Dialer: func(addr string, timeout time.Duration) (net.Conn, error) {
					conn, dialErr := net.DialTimeout("tcp", dest, timeout)
					if dialErr != nil {
						return nil, dialErr
					}
					tlsConn := tls.Client(conn, clientTLSConfig)
					return tlsConn, tlsConn.Handshake()
				},
			}

			client, dialErr := rpc.Dial(*capture, clientOpts)
			if dialErr != nil {
				log.Fatalf("Unable to connect to query leader at %v: %v", leader, dialErr)
			}
			clients = append(clients, client)
			log.Debugf("Handling queries for: %v", leader)
		}
		registerQueryHandler = func(partition int, query planner.QueryClusterFN) {
			minWaitTime := 50 * time.Millisecond
			maxWaitTime := 5 * time.Second

			for i := 0; i < len(leaders); i++ {
				client := clients[i]
				for j := 0; j < 20; j++ { // TODO: make concurrency tunable and don't fail if there are ongoing queries
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
		Dir:                        *dbdir,
		SchemaFile:                 *cmd.Schema,
		EnableGeo:                  *cmd.EnableGeo,
		ISPProvider:                cmd.ISPProvider(),
		AliasesFile:                *cmd.AliasesFile,
		RedisClient:                cmd.RedisClient(),
		RedisCacheSize:             *cmd.RedisCacheSize,
		VirtualTime:                *vtime,
		WALSyncInterval:            *walSync,
		MaxWALSize:                 *maxWALSize,
		WALCompressionSize:         *walCompressionSize,
		MaxMemoryRatio:             *maxMemory,
		Passthrough:                *passthrough,
		NumPartitions:              *numPartitions,
		Partition:                  *partition,
		Follow:                     follow,
		MaxFollowAge:               *maxFollowAge,
		RegisterRemoteQueryHandler: registerQueryHandler,
	})
	db.HandleShutdownSignal()

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
	err := rpcserver.Serve(db, l, &rpcserver.Opts{
		Password: *password,
	})
	if err != nil {
		log.Fatalf("Error serving gRPC: %v", err)
	}
}

func serveHTTP(db *zenodb.DB, hl net.Listener) {
	router := mux.NewRouter()
	err := web.Configure(db, router, &web.Opts{
		OAuthClientID:     *oauthClientID,
		OAuthClientSecret: *oauthClientSecret,
		GitHubOrg:         *gitHubOrg,
		HashKey:           *cookieHashKey,
		BlockKey:          *cookieBlockKey,
		Password:          *password,
		CacheDir:          filepath.Join(*dbdir, "_webcache"),
	})
	if err != nil {
		log.Errorf("Unable to configure web: %v", err)
		return
	}
	http.Serve(hl, router)
}
