package web

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/gorilla/mux"
	"github.com/gorilla/securecookie"
)

var (
	log = golog.LoggerFor("zenodb.web")
)

type Opts struct {
	OAuthClientID         string
	OAuthClientSecret     string
	GitHubOrg             string
	HashKey               string
	BlockKey              string
	CacheDir              string
	CacheTTL              time.Duration
	Password              string
	QueryTimeout          time.Duration
	QueryConcurrencyLimit int
	MaxResponseBytes      int
}

type handler struct {
	Opts
	db               *zenodb.DB
	fs               http.Handler
	sc               *securecookie.SecureCookie
	client           *http.Client
	cache            *cache
	queries          chan *query
	coalescedQueries chan []*query
}

func Configure(db *zenodb.DB, router *mux.Router, opts *Opts) error {
	if opts.OAuthClientID == "" || opts.OAuthClientSecret == "" || opts.GitHubOrg == "" {
		log.Errorf("WARNING - Missing OAuthClientID, OAuthClientSecret and/or GitHubOrg, web API will not authenticate!")
	}

	if opts.CacheDir == "" {
		return errors.New("Unable to start web server, no CacheDir specified")
	}

	if opts.CacheTTL <= 0 {
		opts.CacheTTL = 2 * time.Hour
	}

	if opts.QueryTimeout <= 0 {
		opts.QueryTimeout = 30 * time.Minute
	}

	if opts.QueryConcurrencyLimit <= 0 {
		opts.QueryConcurrencyLimit = 2
	}

	if opts.MaxResponseBytes <= 0 {
		opts.MaxResponseBytes = 25 * 1024 * 1024 // 25 MB
	}

	hashKey := []byte(opts.HashKey)
	blockKey := []byte(opts.BlockKey)

	if len(hashKey) != 64 {
		log.Debugf("Generating random hash key")
		hashKey = make([]byte, 64)
		_, err := rand.Read(hashKey)
		if err != nil {
			return fmt.Errorf("Unable to generate random hash key: %v", err)
		}
	}

	if len(blockKey) != 32 {
		log.Debugf("Generating random block key")
		blockKey = make([]byte, 32)
		_, err := rand.Read(blockKey)
		if err != nil {
			return fmt.Errorf("Unable to generate random block key: %v", err)
		}
	}

	cache, err := newCache(opts.CacheDir, opts.CacheTTL)
	if err != nil {
		return err
	}

	h := &handler{
		Opts:             *opts,
		db:               db,
		sc:               securecookie.New(hashKey, blockKey),
		client:           &http.Client{},
		cache:            cache,
		queries:          make(chan *query, opts.QueryConcurrencyLimit*1000),
		coalescedQueries: make(chan []*query, opts.QueryConcurrencyLimit),
	}

	log.Debugf("Starting %d goroutines to process queries", opts.QueryConcurrencyLimit)
	go h.coalesceQueries()
	for i := 0; i < opts.QueryConcurrencyLimit; i++ {
		go h.processQueries()
	}

	router.StrictSlash(true)
	router.HandleFunc("/insert/{stream}", h.insert)
	router.HandleFunc("/oauth/code", h.oauthCode)
	router.PathPrefix("/async").HandlerFunc(h.asyncQuery)
	router.PathPrefix("/immediate").HandlerFunc(h.immediateQuery)
	router.PathPrefix("/run").HandlerFunc(h.runQuery)
	router.PathPrefix("/cached/{permalink}").HandlerFunc(h.cachedQuery)
	router.PathPrefix("/favicon").Handler(http.NotFoundHandler())
	router.PathPrefix("/report/{permalink}").HandlerFunc(h.index)
	router.PathPrefix("/metrics").HandlerFunc(h.metrics)
	router.PathPrefix("/").HandlerFunc(h.index)

	return nil
}

func buildURL(base string, params map[string]string) (*url.URL, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	for key, value := range params {
		q.Set(key, value)
	}
	u.RawQuery = q.Encode()

	return u, nil
}
