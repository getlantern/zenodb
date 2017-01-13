package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/getlantern/goexpr/isp"
	"github.com/getlantern/goexpr/isp/ip2location"
	"github.com/getlantern/goexpr/isp/maxmind"
	"github.com/getlantern/golog"
	lredis "github.com/getlantern/redis"
	"github.com/getlantern/tlsdefaults"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/planner"
	"github.com/getlantern/zenodb/rpc"
	"github.com/getlantern/zenodb/web"
	"github.com/gorilla/mux"
	"github.com/vharitonsky/iniflags"
	"golang.org/x/net/context"
	"gopkg.in/redis.v5"
)

var (
	log = golog.LoggerFor("zeno")

	dbdir              = flag.String("dbdir", "zenodata", "The directory in which to store the database files, defaults to ./zenodata")
	schema             = flag.String("schema", "schema.yaml", "Location of schema file, defaults to ./schema.yaml")
	aliasesFile        = flag.String("aliases", "", "Optionally specify the path to a file containing expression aliases in the form alias=template(%v,%v) with one alias per line")
	enablegeo          = flag.Bool("enablegeo", false, "enable geolocation functions")
	ispformat          = flag.String("ispformat", "ip2location", "ip2location or maxmind")
	ispdb              = flag.String("ispdb", "", "In order to enable ISP functions, point this to a ISP database file, either in IP2Location Lite format or MaxMind GeoIP2 ISP format")
	vtime              = flag.Bool("vtime", false, "Set this flag to use virtual instead of real time. When using virtual time, the advancement of time will be governed by the timestamps received via insterts.")
	walSync            = flag.Duration("walsync", 5*time.Second, "How frequently to sync the WAL to disk. Set to 0 to sync after every write. Defaults to 5 seconds.")
	maxWALSize         = flag.Int("maxwalsize", 1024*1024*1024, "Maximum size of WAL segments on disk. Defaults to 1 GB.")
	walCompressionSize = flag.Int("walcompressionsize", 30*1024*1024, "Size above which to start compressing WAL segments with snappy. Defaults to 30 MB.")
	maxMemory          = flag.Float64("maxmemory", 0.7, "Set to a non-zero value to cap the total size of the process as a percentage of total system memory. Defaults to 0.7 = 70%.")
	addr               = flag.String("addr", "localhost:17712", "The address at which to listen for gRPC over TLS connections, defaults to localhost:17712")
	httpsAddr          = flag.String("httpsaddr", "localhost:17713", "The address at which to listen for JSON over HTTPS connections, defaults to localhost:17713")
	pprofAddr          = flag.String("pprofaddr", "localhost:4000", "if specified, will listen for pprof connections at the specified tcp address")
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
	redisAddr          = flag.String("redis", "", "Redis address in \"redis[s]://host:port\" format")
	redisCA            = flag.String("redisca", "", "Certificate for redislabs's CA")
	redisClientPK      = flag.String("redisclientpk", "", "Private key for authenticating client to redis's stunnel")
	redisClientCert    = flag.String("redisclientcert", "", "Certificate for authenticating client to redis's stunnel")
	redisCacheSize     = flag.Int("rediscachesize", 25000, "Configures the maximum size of redis caches for HGET operations, defaults to 25,000 per hash")
)

func main() {
	iniflags.Parse()

	if *pprofAddr != "" {
		go func() {
			log.Debugf("Starting pprof page at http://%s/debug/pprof", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Error(err)
			}
		}()
	}

	l, err := tlsdefaults.Listen(*addr, *pkfile, *certfile)
	if err != nil {
		log.Fatalf("Unable to listen for gRPC over TLS connections at %v: %v", *addr, err)
	}

	hl, err := tlsdefaults.Listen(*httpsAddr, *pkfile, *certfile)
	if err != nil {
		log.Fatalf("Unable to listen for HTTPS connections at %v: %v", *httpsAddr, err)
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

	clientSessionCache := tls.NewLRUClientSessionCache(10000)
	var follow func(f *zenodb.Follow, cb func(data []byte, newOffset wal.Offset) error)
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
		follow = func(f *zenodb.Follow, insert func(data []byte, newOffset wal.Offset) error) {
			minWait := 1 * time.Second
			maxWait := 1 * time.Minute
			wait := minWait
			for {
				for {
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
						f.Offset = newOffset
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

	var redisClient *redis.Client
	if *redisAddr != "" {
		log.Debugf("Connecting to Redis at %v", *redisAddr)
		redisClient, err = lredis.NewClient(&lredis.Opts{
			RedisURL:       *redisAddr,
			RedisCAFile:    *redisCA,
			ClientPKFile:   *redisClientPK,
			ClientCertFile: *redisClientCert,
		})
		if err == nil {
			log.Debugf("Connected to Redis at %v", *redisAddr)
		} else {
			log.Errorf("Unable to connect to redis: %v", err)
		}
	}

	db, err := zenodb.NewDB(&zenodb.DBOpts{
		Dir:                        *dbdir,
		SchemaFile:                 *schema,
		EnableGeo:                  *enablegeo,
		ISPProvider:                ispProvider,
		AliasesFile:                *aliasesFile,
		RedisClient:                redisClient,
		RedisCacheSize:             *redisCacheSize,
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
	err := rpc.Serve(db, l, &rpc.ServerOpts{
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
	})
	if err != nil {
		log.Errorf("Unable to configure web: %v", err)
		return
	}
	http.Serve(hl, router)
}
