package server

import (
	"crypto/rand"
	"crypto/tls"
	serrors "errors"
	"flag"
	"io/ioutil"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/getlantern/errors"
	"github.com/getlantern/golog"
	"github.com/getlantern/tlsdefaults"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/cmd"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/planner"
	"github.com/getlantern/zenodb/rpc"
	rpcserver "github.com/getlantern/zenodb/rpc/server"
	"github.com/getlantern/zenodb/web"
	"github.com/gorilla/mux"
	"golang.org/x/crypto/acme/autocert"
	"golang.org/x/net/context"
)

const (
	DefaultClusterQueryConcurrency = 25
	DefaultNextQueryTimeout        = 5 * time.Minute

	sourceIDDelim = "|"
)

var (
	ErrInvalidID      = serrors.New("id below 0")
	ErrMissingID      = serrors.New("clustered server missing id")
	ErrAlreadyRunning = serrors.New("already running")
)

// Server is a zeno server (standalone, leader of follower)
type Server struct {
	DBDir                     string
	Vtime                     bool
	WALSync                   time.Duration
	MaxWALSize                int
	WALCompressionSize        int
	MaxMemory                 float64
	IterationCoalesceInterval time.Duration
	IterationConcurrency      int
	Addr                      string
	Listener                  net.Listener
	HTTPAddr                  string
	HTTPListener              net.Listener
	HTTPSAddr                 string
	HTTPSListener             net.Listener
	Router                    *mux.Router
	Password                  string
	PKFile                    string
	CertFile                  string
	CookieHashKey             string
	CookieBlockKey            string
	OauthClientID             string
	OauthClientSecret         string
	GitHubOrg                 string
	Insecure                  bool
	Passthrough               bool
	Capture                   string
	CaptureOverride           string
	Feed                      string
	FeedOverride              string
	ID                        int
	AllowZeroID               bool
	NumPartitions             int
	Partition                 int
	ClusterQueryConcurrency   int
	ClusterQueryTimeout       time.Duration
	NextQueryTimeout          time.Duration
	MaxFollowAge              time.Duration
	TLSDomain                 string
	WebQueryCacheTTL          time.Duration
	WebQueryTimeout           time.Duration
	WebQueryConcurrencyLimit  int
	WebMaxResponseBytes       int
	ListenTimeout             time.Duration
	MaxReconnectWaitTime      time.Duration
	Panic                     func(err interface{})

	Schema         string
	AliasesFile    string
	EnableGeo      bool
	RedisCacheSize int

	log     golog.Logger
	db      *zenodb.DB
	stopRPC func()
	stopWeb func()

	running   bool
	runningMx sync.Mutex
}

// Prepare prepares the zeno server to run, returning a reference to the underlying db and a blocking function for actually running.
func (s *Server) Prepare() (*zenodb.DB, func() error, error) {
	if s.ID < 0 {
		return nil, nil, ErrInvalidID
	}
	if s.ID == 0 && s.NumPartitions > 0 && !s.AllowZeroID {
		return nil, nil, ErrMissingID
	}

	s.runningMx.Lock()
	if s.running {
		return nil, nil, ErrAlreadyRunning
	}

	if s.ClusterQueryConcurrency <= 0 {
		s.ClusterQueryConcurrency = DefaultClusterQueryConcurrency
	}
	if s.NextQueryTimeout <= 0 {
		s.NextQueryTimeout = DefaultNextQueryTimeout
	}

	dbOpts := &zenodb.DBOpts{
		Dir:                       s.DBDir,
		SchemaFile:                s.Schema,
		EnableGeo:                 s.EnableGeo,
		ISPProvider:               cmd.ISPProvider(),
		AliasesFile:               s.AliasesFile,
		RedisClient:               cmd.RedisClient(),
		RedisCacheSize:            s.RedisCacheSize,
		VirtualTime:               s.Vtime,
		WALSyncInterval:           s.WALSync,
		MaxWALSize:                s.MaxWALSize,
		WALCompressionSize:        s.WALCompressionSize,
		MaxMemoryRatio:            s.MaxMemory,
		IterationCoalesceInterval: s.IterationCoalesceInterval,
		Passthrough:               s.Passthrough,
		ID:                        s.ID,
		NumPartitions:             s.NumPartitions,
		Partition:                 s.Partition,
		ClusterQueryConcurrency:   s.ClusterQueryConcurrency,
		ClusterQueryTimeout:       s.ClusterQueryTimeout,
		MaxFollowAge:              s.MaxFollowAge,
		Panic:                     s.Panic,
	}

	s.log = dbOpts.BuildLogger()
	s.log.Debug("Starting")

	clientSessionCache := tls.NewLRUClientSessionCache(10000)
	if s.Capture != "" {
		clients, err := s.clientsFor(s.Capture, s.CaptureOverride, clientSessionCache)
		if err != nil {
			return nil, nil, err
		}
		sources := make([]int, 0, len(clients))
		for source := range clients {
			sources = append(sources, source)
		}
		s.log.Debugf("Capturing data from %v", s.Capture)
		dbOpts.Follow = func(ff func([]int) map[int]*common.Follow, insert func(data []byte, newOffset wal.Offset, source int) error) {
			s.follow(clients, sources, ff, insert)
		}
	}

	if s.Feed != "" {
		clients, err := s.clientsFor(s.Feed, s.FeedOverride, clientSessionCache)
		if err != nil {
			return nil, nil, err
		}
		s.log.Debugf("Handling queries for: %v", s.Feed)
		dbOpts.RegisterRemoteQueryHandler = func(db *zenodb.DB, partition int, query planner.QueryClusterFN) {
			minWaitTime := 50 * time.Millisecond
			maxWaitTime := s.MaxReconnectWaitTime
			if maxWaitTime <= 0 {
				maxWaitTime = 5 * time.Second
			}

			for _, _client := range clients {
				client := _client
				for j := 0; j < s.ClusterQueryConcurrency; j++ { // TODO: don't fail if there are ongoing queries past the allowed concurrency
					db.Go(func(stop <-chan interface{}) {
						go func() {
							<-stop
							client.Close()
						}()

						// Continually handle queries and then reconnect for next query
						waitTime := minWaitTime
						for {
							handleErr := client.ProcessRemoteQuery(context.Background(), partition, query, s.NextQueryTimeout)
							if handleErr == nil {
								waitTime = minWaitTime
							} else {
								s.log.Debugf("Error handling queries: %v", handleErr)
								// Exponential back-off
								select {
								case <-stop:
									return
								case <-time.After(waitTime):
									waitTime *= 2
									if waitTime > maxWaitTime {
										waitTime = maxWaitTime
									}
								}
							}
						}
					})
				}
			}
		}
	}

	var err error
	s.db, err = zenodb.NewDB(dbOpts)
	if err != nil {
		return nil, nil, s.log.Errorf("Unable to open database at %v: %v", s.DBDir, err)
	}
	s.log.Debugf("Opened database at %v\n", s.DBDir)

	run := func() error {
		defer func() {
			if s.Listener != nil {
				s.Listener.Close()
				s.Listener = nil
			}
			if s.HTTPListener != nil {
				s.HTTPListener.Close()
				s.HTTPListener = nil
			}
			if s.HTTPSListener != nil {
				s.HTTPSListener.Close()
				s.HTTPSListener = nil
			}
		}()

		if s.Listener == nil {
			s.log.Debugf("Starting listener for %v", s.Addr)
			err = s.listen(func() error {
				s.Listener, err = tlsdefaults.Listen(s.Addr, s.PKFile, s.CertFile)
				return err
			})
			if err != nil {
				return s.log.Errorf("Unable to listen for gRPC over TLS connections at %v: %v", s.Addr, err)
			}
		}
		s.log.Debugf("Listening for gRPC connections at %v\n", s.Listener.Addr())

		if s.HTTPListener == nil && s.HTTPAddr != "" {
			err = s.listen(func() error {
				s.HTTPListener, err = net.Listen("tcp", s.HTTPAddr)
				return err
			})
			if err != nil {
				return s.log.Errorf("Unable to listen HTTP: %v", err)
			}
		}
		if s.HTTPListener != nil {
			s.log.Debugf("Listening for HTTP connections at %v\n", s.HTTPListener.Addr())
		}

		if s.HTTPSListener == nil && s.HTTPSAddr != "" {
			if s.TLSDomain != "" {
				m := autocert.Manager{
					Prompt: autocert.AcceptTOS,
					HostPolicy: func(_ context.Context, host string) error {
						// Support any host
						return nil
					},
					Cache:    autocert.DirCache("certs"),
					Email:    "admin@getlantern.org",
					ForceRSA: true, // we need to force RSA keys because CloudFront doesn't like our ECDSA cipher suites
				}
				tlsConfig := &tls.Config{
					GetCertificate:           m.GetCertificate,
					PreferServerCipherSuites: true,
					SessionTicketKey:         s.GetSessionTicketKey(),
				}
				err = s.listen(func() error {
					s.HTTPSListener, err = tls.Listen("tcp", s.HTTPSAddr, tlsConfig)
					return err
				})
				if err != nil {
					return s.log.Errorf("Unable to listen HTTPS: %v", err)
				}
			} else {
				err = s.listen(func() error {
					s.HTTPSListener, err = tlsdefaults.Listen(s.HTTPSAddr, s.PKFile, s.CertFile)
					return err
				})
				if err != nil {
					return s.log.Errorf("Unable to listen for HTTPS connections at %v: %v", s.HTTPSAddr, err)
				}
			}
		}
		if s.HTTPSListener != nil {
			s.log.Debugf("Listening for HTTPS connections at %v\n", s.HTTPSListener.Addr())
		}

		serveHTTP, err := s.serveHTTP()
		if err != nil {
			return s.log.Errorf("Unable to serve HTTP: %v", err)
		}
		s.log.Debug("Serving HTTP")

		errCh := make(chan error, 2)
		go func() {
			errCh <- serveHTTP()
		}()
		go func() {
			errCh <- s.serveRPC()
		}()

		s.running = true
		s.log.Debug("Started")
		s.runningMx.Unlock()

		for i := 0; i < 2; i++ {
			err := <-errCh
			if err != nil {
				return err
			}
		}
		return nil
	}

	return s.db, run, nil
}

func (s *Server) listen(fn func() error) error {
	start := time.Now()
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if s.ListenTimeout <= 0 || time.Now().Sub(start) > s.ListenTimeout {
			return err
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (s *Server) follow(clients map[int]rpc.Client, sources []int, ff func([]int) map[int]*common.Follow, insert func(data []byte, newOffset wal.Offset, source int) error) {
	follows := ff(sources)
	for _source, _f := range follows {
		source := _source
		f := _f
		client := clients[source]
		s.db.Go(func(stop <-chan interface{}) {
			go func() {
				<-stop
				client.Close()
			}()
			s.followSource(client, source, f, insert, stop)
		})
	}
}

func (s *Server) followSource(client rpc.Client, source int, f *common.Follow, insert func(data []byte, newOffset wal.Offset, source int) error, stop <-chan interface{}) {
	s.log.Debugf("Following source %d starting at %v", source, f.EarliestOffset)
	minWait := 1 * time.Second
	maxWait := 1 * time.Minute
	wait := minWait
	var followMx sync.Mutex

	followStreams := func() {
		followMx.Lock()
		source, followFunc, followErr := client.Follow(context.Background(), f)
		followMx.Unlock()
		if followErr != nil {
			s.log.Errorf("Error following stream %v: %v", f.Stream, followErr)
			return
		}

		for {
			data, newOffset, followErr := followFunc()
			if followErr != nil {
				s.log.Errorf("Error reading from stream %v: %v", f.Stream, followErr)
				return
			}
			insertErr := insert(data, newOffset, source)
			if insertErr != nil {
				s.log.Errorf("Error inserting data for stream %v: %v", f.Stream, insertErr)
				return
			}
			followMx.Lock()
			f.EarliestOffset = newOffset
			followMx.Unlock()
			// success, reset wait time
			wait = minWait
		}
	}

	for {
		followStreams()

		// Exponential Backoff
		select {
		case <-stop:
			return
		case <-time.After(wait):
			wait *= 2
			if wait > maxWait {
				wait = maxWait
			}
		}
	}
}

func (s *Server) serveRPC() error {
	serve, stop := rpcserver.PrepareServer(s.db, s.Listener, &rpcserver.Opts{
		ID:       s.ID,
		Password: s.Password,
	})
	s.stopRPC = stop
	if err := serve(); err != nil {
		return errors.New("Error serving gRPC: %v", err)
	}
	return nil
}

func (s *Server) serveHTTP() (func() error, error) {
	if s.Router == nil {
		s.Router = mux.NewRouter()
	}
	stop, err := web.Configure(s.db, s.Router, &web.Opts{
		OAuthClientID:         s.OauthClientID,
		OAuthClientSecret:     s.OauthClientSecret,
		GitHubOrg:             s.GitHubOrg,
		HashKey:               s.CookieHashKey,
		BlockKey:              s.CookieBlockKey,
		Password:              s.Password,
		CacheDir:              filepath.Join(s.DBDir, "_webcache"),
		CacheTTL:              s.WebQueryCacheTTL,
		QueryTimeout:          s.WebQueryTimeout,
		QueryConcurrencyLimit: s.WebQueryConcurrencyLimit,
		MaxResponseBytes:      s.WebMaxResponseBytes,
	})
	if err != nil {
		return nil, err
	}
	s.stopWeb = stop
	return func() error {
		errorsCh := make(chan error, 2)
		hs := &http.Server{
			Handler:        s.Router,
			MaxHeaderBytes: 1 << 19,
		}
		if s.HTTPListener != nil {
			go func() {
				errorsCh <- hs.Serve(s.HTTPListener)
			}()
		}
		if s.HTTPSListener != nil {
			go func() {
				errorsCh <- hs.Serve(s.HTTPSListener)
			}()
		}
		return <-errorsCh
	}, nil
}

// GetSessionTicketKey allows us to reuse a session ticket key across restarts,
// which avoids excessive TLS renegotiation with old clients.
func (s *Server) GetSessionTicketKey() [32]byte {
	var key [32]byte
	keySlice, err := ioutil.ReadFile("session_ticket_key")
	if err != nil {
		keySlice = make([]byte, 32)
		n, err := rand.Read(keySlice)
		if err != nil {
			s.log.Errorf("Unable to generate session ticket key: %v", err)
			return key
		}
		if n != 32 {
			s.log.Errorf("Generated unexpected length of random data %d", n)
			return key
		}
		err = ioutil.WriteFile("session_ticket_key", keySlice, 0600)
		if err != nil {
			s.log.Errorf("Unable to save session_ticket_key: %v", err)
		} else {
			s.log.Debug("Saved new session_ticket_key")
		}
	}
	copy(key[:], keySlice)
	return key
}

func (s *Server) clientsFor(serversString string, serverOverridesString string, clientSessionCache tls.ClientSessionCache) (map[int]rpc.Client, error) {
	servers := strings.Split(serversString, ",")
	serverOverrides := strings.Split(serverOverridesString, ",")
	if s.FeedOverride != "" && len(servers) != len(serverOverrides) {
		return nil, errors.New("Number of overrides must match number of servers")
	}
	clientsByServerID := make(map[int]rpc.Client, len(servers))
	for i, _server := range servers {
		serverAndID := strings.Split(_server, sourceIDDelim)
		var server string
		var id int
		server = serverAndID[0]
		if len(serverAndID) > 1 {
			var parseErr error
			id, parseErr = strconv.Atoi(serverAndID[1])
			if parseErr != nil {
				return nil, errors.New("Error parsing server id %v: %v", serverAndID[1], parseErr)
			}
		}
		host, _, _ := net.SplitHostPort(server)
		clientTLSConfig := &tls.Config{
			ServerName:         host,
			InsecureSkipVerify: s.Insecure,
			ClientSessionCache: clientSessionCache,
		}

		dest := server
		if len(serverOverridesString) > 0 {
			dest = serverOverrides[i]
		}

		clientOpts := &rpc.ClientOpts{
			Password: s.Password,
			Dialer: func(addr string, timeout time.Duration) (net.Conn, error) {
				conn, dialErr := net.DialTimeout("tcp", dest, timeout)
				if dialErr != nil {
					return nil, dialErr
				}
				tlsConn := tls.Client(conn, clientTLSConfig)
				return tlsConn, tlsConn.Handshake()
			},
		}

		client, dialErr := rpc.Dial(s.Capture, clientOpts)
		if dialErr != nil {
			return nil, errors.New("Unable to connect to server %v at %v: %v", server, dest, dialErr)
		}
		clientsByServerID[id] = client
	}

	return clientsByServerID, nil
}

func (s *Server) Close() {
	s.log.Debug("Close requested")
	s.runningMx.Lock()
	if s.running {
		s.log.Debug("Closing")
		s.Listener.Close()
		if s.HTTPListener != nil {
			s.HTTPListener.Close()
		}
		if s.HTTPSListener != nil {
			s.HTTPSListener.Close()
		}
		s.db.Close()
		s.stopRPC()
		s.stopWeb()
		s.running = false
		s.log.Debug("Closed")
	} else {
		s.log.Debug("Not running, don't bother closing")
	}
	s.runningMx.Unlock()
}

func (s *Server) ConfigureFlags() {
	flag.StringVar(&s.DBDir, "dbdir", "zenodata", "The directory in which to store the database files, defaults to ./zenodata")
	flag.BoolVar(&s.Vtime, "vtime", false, "Set this flag to use virtual instead of real time. When using virtual time, the advancement of time will be governed by the timestamps received via inserts.")
	flag.DurationVar(&s.WALSync, "walsync", 5*time.Second, "How frequently to sync the WAL to disk. Set to 0 to sync after every write. Defaults to 5 seconds.")
	flag.IntVar(&s.MaxWALSize, "maxwalsize", 1024*1024*1024, "Maximum size of WAL segments on disk. Defaults to 1 GB.")
	flag.IntVar(&s.WALCompressionSize, "walcompressionsize", 30*1024*1024, "Size above which to start compressing WAL segments with snappy. Defaults to 30 MB.")
	flag.Float64Var(&s.MaxMemory, "maxmemory", 0.7, "Set to a non-zero value to cap the total size of the process as a percentage of total system memory. Defaults to 0.7 = 70%.")
	flag.DurationVar(&s.IterationCoalesceInterval, "itercoalesce", zenodb.DefaultIterationCoalesceInterval, "Period to wait for coalescing parallel iterations")
	flag.IntVar(&s.IterationConcurrency, "iterconcurrency", zenodb.DefaultIterationConcurrency, "specifies the maximum concurrency for iterating tables")
	flag.StringVar(&s.Addr, "addr", "localhost:17712", "The address at which to listen for gRPC over TLS connections, defaults to localhost:17712")
	flag.StringVar(&s.HTTPSAddr, "httpsaddr", "localhost:17713", "The address at which to listen for JSON over HTTPS connections, defaults to localhost:17713")
	flag.StringVar(&s.HTTPAddr, "httpaddr", "", "The address at which to listen for JSON over HTTP connections, defaults to localhost:17713")
	flag.StringVar(&s.Password, "password", "", "if specified, will authenticate clients using this password")
	flag.StringVar(&s.PKFile, "pkfile", "pk.pem", "path to the private key PEM file")
	flag.StringVar(&s.CertFile, "certfile", "cert.pem", "path to the certificate PEM file")
	flag.StringVar(&s.CookieHashKey, "cookiehashkey", "", "key to use for HMAC authentication of web auth cookies, should be 64 bytes, defaults to random 64 bytes if not specified")
	flag.StringVar(&s.CookieBlockKey, "cookieblockkey", "", "key to use for encrypting web auth cookies, should be 32 bytes, defaults to random 32 bytes if not specified")
	flag.StringVar(&s.OauthClientID, "oauthclientid", "", "id to use for oauth client to connect to GitHub")
	flag.StringVar(&s.OauthClientSecret, "oauthclientsecret", "", "secret id to use for oauth client to connect to GitHub")
	flag.StringVar(&s.GitHubOrg, "githuborg", "", "the GitHug org against which web users are authenticated")
	flag.BoolVar(&s.Insecure, "insecure", false, "set to true to disable TLS certificate verification when connecting to other zeno servers (don't use this in production!)")
	flag.BoolVar(&s.Passthrough, "passthrough", false, "set to true to make this node a passthrough that doesn't capture data in table but is capable of feeding and querying other nodes. requires that -partitions be specified.")
	flag.StringVar(&s.Capture, "capture", "", "if specified, connect to the node at the given address to receive updates, authenticating with value of -password.  requires that you specify which -partition this node handles.")
	flag.StringVar(&s.CaptureOverride, "captureoverride", "", "if specified, dial network connection for -capture using this address, but verify TLS connection using the address from -capture")
	flag.StringVar(&s.Feed, "feed", "", "if specified, connect to the nodes at the given comma,delimited addresses to handle queries for them, authenticating with value of -password. requires that you specify which -partition this node handles.")
	flag.StringVar(&s.FeedOverride, "feedoverride", "", "if specified, dial network connection for -feed using this address, but verify TLS connection using the address from -feed")
	flag.IntVar(&s.ID, "id", 0, "unique identifier for a leader. if running in a cluster and omitting ID or specifying id = 0, you need to also specify the -allowzeroid flag")
	flag.BoolVar(&s.AllowZeroID, "allowzeroid", false, "specify this flag to allow omitting the -id parameter or setting it to 0")
	flag.IntVar(&s.NumPartitions, "numpartitions", 1, "The number of partitions available to distribute amongst followers")
	flag.IntVar(&s.Partition, "partition", 0, "the partition number assigned to this follower")
	flag.IntVar(&s.ClusterQueryConcurrency, "clusterqueryconcurrency", DefaultClusterQueryConcurrency, "specifies the maximum concurrency for clustered queries")
	flag.DurationVar(&s.ClusterQueryTimeout, "clusterquerytimeout", zenodb.DefaultClusterQueryTimeout, "specifies the maximum time leader will wait for followers to answer a query")
	flag.DurationVar(&s.NextQueryTimeout, "nextquerytimeout", DefaultNextQueryTimeout, "specifies the maximum time follower will wait for leader to send a query on an open connection")
	flag.DurationVar(&s.MaxFollowAge, "maxfollowage", 0, "user with -follow, limits how far to go back when pulling data from leader")
	flag.StringVar(&s.TLSDomain, "tlsdomain", "", "Specify this to automatically use LetsEncrypt certs for this domain")
	flag.DurationVar(&s.WebQueryCacheTTL, "webquerycachettl", 2*time.Hour, "specifies how long to cache web query results")
	flag.DurationVar(&s.WebQueryTimeout, "webquerytimeout", 30*time.Minute, "time out web queries after this duration")
	flag.IntVar(&s.WebQueryConcurrencyLimit, "webqueryconcurrency", 2, "limit concurrent web queries to this (subsequent queries will be queued)")
	flag.IntVar(&s.WebMaxResponseBytes, "webquerymaxresponsebytes", 25*1024*1024, "limit the size of query results returned through the web API")
}
