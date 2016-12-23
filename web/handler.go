package web

import (
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/gorilla/mux"
	"github.com/gorilla/securecookie"
	"net/http"
	"net/url"
	"time"
)

var (
	log = golog.LoggerFor("zenodb.web")
)

type Opts struct {
	OAuthClientID      string
	OAuthClientSecret  string
	GitHubOrg          string
	HashKey            string
	BlockKey           string
	CacheTTL           time.Duration
	MaxCacheBytes      int
	MaxCacheEntryBytes int
}

type handler struct {
	Opts
	db     *zenodb.DB
	fs     http.Handler
	sc     *securecookie.SecureCookie
	client *http.Client
	cache  *cache
}

func Configure(db *zenodb.DB, router *mux.Router, opts *Opts) error {
	if opts.OAuthClientID == "" || opts.OAuthClientSecret == "" {
		return errors.New("Unable to start web server, missing OAuthClientID and/or OAuthClientSecret")
	}

	if opts.GitHubOrg == "" {
		return errors.New("Unable to start web server, no GitHubOrg specified")
	}

	if opts.CacheTTL == 0 {
		opts.CacheTTL = 1 * time.Hour
	}

	if opts.MaxCacheBytes <= 0 {
		opts.MaxCacheBytes = 100 * 1024 * 1204
	}

	if opts.MaxCacheEntryBytes <= 0 {
		opts.MaxCacheEntryBytes = opts.MaxCacheBytes / 20
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

	h := &handler{
		Opts:   *opts,
		db:     db,
		sc:     securecookie.New(hashKey, blockKey),
		client: &http.Client{},
		cache:  newCache(opts.CacheTTL, opts.MaxCacheBytes),
	}

	router.StrictSlash(true)
	router.HandleFunc("/insert/{stream}", h.insert)
	router.HandleFunc("/oauth/code", h.oauthCode)
	router.PathPrefix("/run").HandlerFunc(h.runQuery)
	router.PathPrefix("/favicon").Handler(http.NotFoundHandler())
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
