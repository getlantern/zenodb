package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/getlantern/goexpr/isp"
	"github.com/getlantern/goexpr/isp/ip2location"
	"github.com/getlantern/goexpr/isp/maxmind"
	"github.com/getlantern/golog"

	"strings"

	rclient "github.com/go-redis/redis"
)

var (
	log = golog.LoggerFor("cmd")
)

var (
	Schema         = flag.String("schema", "schema.yaml", "Location of schema file, defaults to ./schema.yaml")
	AliasesFile    = flag.String("aliases", "", "Optionally specify the path to a file containing expression aliases in the form alias=template(%v,%v) with one alias per line")
	EnableGeo      = flag.Bool("enablegeo", false, "enable geolocation functions")
	ISPFormat      = flag.String("ispformat", "ip2location", "ip2location or maxmind")
	ISPDB          = flag.String("ispdb", "", "In order to enable ISP functions, point this to a ISP database file, either in IP2Location Lite format or MaxMind GeoIP2 ISP format")
	redisAddr      = flag.String("redis", "", "Redis address in \"redis[s]://host:port\" format")
	redisPassword  = flag.String("redispassword", "", "Password for accessing redis")
	redisCA        = flag.String("redisca", "", "Certificate for the remote redis's CA")
	RedisCacheSize = flag.Int("rediscachesize", 25000, "Configures the maximum size of redis caches for HGET operations, defaults to 25,000 per hash")
	PprofAddr      = flag.String("pprofaddr", "localhost:4000", "if specified, will listen for pprof connections at the specified tcp address")
)

func StartPprof() {
	if *PprofAddr != "" {
		go func() {
			log.Debugf("Starting pprof page at http://%s/debug/pprof", *PprofAddr)
			if err := http.ListenAndServe(*PprofAddr, nil); err != nil {
				log.Errorf("Unable to start PPROF HTTP interface: %v", err)
			}
		}()
	}
}

func ISPProvider() isp.Provider {
	if *ISPFormat == "" || *ISPDB == "" {
		log.Debug("ISP provider not configured")
		return nil
	}

	log.Debugf("Enabling ISP functions using format %v with db file at %v", *ISPFormat, *ISPDB)
	var ispProvider isp.Provider
	var providerErr error
	switch strings.ToLower(strings.TrimSpace(*ISPFormat)) {
	case "ip2location":
		ispProvider, providerErr = ip2location.NewProvider(*ISPDB)
	case "maxmind":
		ispProvider, providerErr = maxmind.NewProvider(*ISPDB)
	default:
		log.Errorf("Unknown ispdb format %v", *ISPFormat)
	}
	if providerErr != nil {
		log.Errorf("Unable to initialize ISP provider %v from %v: %v", *ISPFormat, *ISPDB, providerErr)
		ispProvider = nil
	}

	return ispProvider
}

// RedisClient creates a new redis client.
func RedisClient() *rclient.Client {
	var redisErr error
	opts := &rclient.Options{
		PoolSize:     10,
		ReadTimeout:  1 * time.Minute,
		WriteTimeout: 1 * time.Minute,
		IdleTimeout:  4 * time.Minute,
		Password:     *redisPassword,
		Addr:         *redisAddr,
		DialTimeout:  30 * time.Second,
	}

	var cert []byte
	if _, err := os.Stat(*redisCA); os.IsNotExist(err) {
		cert = []byte(*redisCA)
	} else {
		cert, err = ioutil.ReadFile(*redisCA)
		if err != nil {
			return nil, errors.New("Error reading cert: %v", err)
		}
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(cert)
	opts.TLSConfig = &tls.Config{
		RootCAs: certPool,
	}
	client := rclient.NewClient(opts)

	if e := client.Ping().Err(); e != nil {
		return nil, errors.New("Error pinging redis: %v", e)
	}
	return client, nil
}
