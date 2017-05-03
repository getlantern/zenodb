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
	OAuthClientID     string
	OAuthClientSecret string
	GitHubOrg         string
	HashKey           string
	BlockKey          string
	CacheDir          string
	CacheTTL          time.Duration
	Password          string
	QueryTimeout      time.Duration
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

	if opts.CacheDir == "" {
		return errors.New("Unable to start web server, no CacheDir specified")
	}

	if opts.CacheTTL == 0 {
		opts.CacheTTL = 1 * time.Hour
	}

	if opts.QueryTimeout == 0 {
		opts.QueryTimeout = 10 * time.Minute
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
		Opts:   *opts,
		db:     db,
		sc:     securecookie.New(hashKey, blockKey),
		client: &http.Client{},
		cache:  cache,
	}

	router.StrictSlash(true)
	router.HandleFunc("/insert/{stream}", h.insert)
	router.HandleFunc("/oauth/code", h.oauthCode)
	router.PathPrefix("/async").HandlerFunc(h.asyncQuery)
	router.PathPrefix("/run").HandlerFunc(h.runQuery)
	router.PathPrefix("/nocache").HandlerFunc(h.noCache)
	router.PathPrefix("/cached/{permalink}").HandlerFunc(h.cachedQuery)
	router.PathPrefix("/favicon").Handler(http.NotFoundHandler())
	router.PathPrefix("/report/{permalink}").HandlerFunc(h.index)
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
