package server

import (
	"crypto/rand"
	"crypto/tls"
	serrors "errors"
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
	HTTPSAddr                 string
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
	RPCKeepaliveInterval      time.Duration
	RPCKeepAliveTimeout       time.Duration
	ListenTimeout             time.Duration
	MaxReconnectWaitTime      time.Duration
	Panic                     func(err interface{})

	Schema         string
	AliasesFile    string
	EnableGeo      bool
	RedisCacheSize int

	log     golog.Logger
	db      *zenodb.DB
	l       net.Listener
	hl      net.Listener
	stopRPC func()
	stopWeb func()

	running   bool
	runningMx sync.Mutex
}

// Serve runs the zeno server. The returned function allows waiting until the server is finished running and returns the first error encountered while running.
func (s *Server) Serve() (func() error, error) {
	if s.ID < 0 {
		return nil, ErrInvalidID
	}
	if s.ID == 0 && s.NumPartitions > 0 && !s.AllowZeroID {
		return nil, ErrMissingID
	}

	s.runningMx.Lock()
	started := false
	defer func() {
		if !started {
			if s.l != nil {
				s.l.Close()
				s.l = nil
			}
			if s.hl != nil {
				s.hl.Close()
				s.hl = nil
			}
			s.runningMx.Unlock()
		}
	}()

	if s.running {
		return nil, ErrAlreadyRunning
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
			return nil, err
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
			return nil, err
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
		return nil, s.log.Errorf("Unable to open database at %v: %v", s.DBDir, err)
	}
	s.log.Debugf("Opened database at %v\n", s.DBDir)

	err = s.listen(func() error {
		s.l, err = tlsdefaults.Listen(s.Addr, s.PKFile, s.CertFile)
		return err
	})
	if err != nil {
		return nil, s.log.Errorf("Unable to listen for gRPC over TLS connections at %v: %v", s.Addr, err)
	}
	s.log.Debugf("Listening for gRPC connections at %v\n", s.l.Addr())

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
			SessionTicketKey:         s.getSessionTicketKey(),
		}
		err = s.listen(func() error {
			s.hl, err = tls.Listen("tcp", s.HTTPSAddr, tlsConfig)
			return err
		})
		if err != nil {
			return nil, s.log.Errorf("Unable to listen HTTPS: %v", err)
		}
	} else {
		err = s.listen(func() error {
			s.hl, err = tlsdefaults.Listen(s.HTTPSAddr, s.PKFile, s.CertFile)
			return err
		})
		if err != nil {
			return nil, s.log.Errorf("Unable to listen for HTTPS connections at %v: %v", s.HTTPSAddr, err)
		}
	}
	s.log.Debugf("Listening for HTTPS connections at %v\n", s.hl.Addr())

	serveHTTP, err := s.serveHTTP()
	if err != nil {
		return nil, s.log.Errorf("Unable to serve HTTP: %v", err)
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
	started = true
	s.log.Debug("Started")
	s.runningMx.Unlock()

	return func() error {
		for i := 0; i < 2; i++ {
			err := <-errCh
			if err != nil {
				return err
			}
		}
		return nil
	}, nil
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
	serve, stop := rpcserver.PrepareServer(s.db, s.l, &rpcserver.Opts{
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
	router := mux.NewRouter()
	stop, err := web.Configure(s.db, router, &web.Opts{
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
		return http.Serve(s.hl, router)
	}, nil
}

// this allows us to reuse a session ticket key across restarts, which avoids
// excessive TLS renegotiation with old clients.
func (s *Server) getSessionTicketKey() [32]byte {
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
			KeepaliveInterval: s.RPCKeepaliveInterval,
			KeepaliveTimeout:  s.RPCKeepAliveTimeout,
		}

		client, dialErr := rpc.Dial(s.Capture, clientOpts)
		if dialErr != nil {
			return nil, errors.New("Unable to connect to server at %v: %v", server, dialErr)
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
		s.l.Close()
		s.hl.Close()
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
