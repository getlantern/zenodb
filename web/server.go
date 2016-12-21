package web

import (
	"crypto/rand"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/gorilla/mux"
	"github.com/gorilla/securecookie"
	"net"
	"net/http"
	"net/url"
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
}

type server struct {
	Opts
	db     *zenodb.DB
	fs     http.Handler
	sc     *securecookie.SecureCookie
	client *http.Client
}

func Serve(db *zenodb.DB, l net.Listener, opts *Opts) {
	if opts.OAuthClientID == "" || opts.OAuthClientSecret == "" {
		log.Error("Unable to start web server, missing OAuthClientID and/or OAuthClientSecret")
		return
	}

	if opts.GitHubOrg == "" {
		log.Error("Unable to start web server, no GitHubOrg specified")
		return
	}

	hashKey := []byte(opts.HashKey)
	blockKey := []byte(opts.BlockKey)

	if len(hashKey) != 64 {
		log.Debugf("Generating random hash key")
		hashKey = make([]byte, 64)
		_, err := rand.Read(hashKey)
		if err != nil {
			panic(err)
		}
	}

	if len(blockKey) != 32 {
		log.Debugf("Generating random block key")
		blockKey = make([]byte, 32)
		_, err := rand.Read(blockKey)
		if err != nil {
			panic(err)
		}
	}

	s := &server{
		Opts:   *opts,
		db:     db,
		sc:     securecookie.New(hashKey, blockKey),
		client: &http.Client{},
	}

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/oauth/code", s.oauthCode)
	router.PathPrefix("/run").HandlerFunc(s.runQuery)
	router.PathPrefix("/").HandlerFunc(s.index)

	err := http.Serve(l, router)
	if err != nil {
		panic(err)
	}
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
