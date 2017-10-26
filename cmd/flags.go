package cmd

import (
	"flag"
	"net/http"
	_ "net/http/pprof"

	"github.com/getlantern/goexpr/isp"
	"github.com/getlantern/goexpr/isp/ip2location"
	"github.com/getlantern/goexpr/isp/maxmind"
	"github.com/getlantern/golog"
	tlsredis "github.com/getlantern/tlsredis"
	"gopkg.in/redis.v5"
	"strings"
)

var (
	log = golog.LoggerFor("cmd")
)

var (
	Schema          = flag.String("schema", "schema.yaml", "Location of schema file, defaults to ./schema.yaml")
	AliasesFile     = flag.String("aliases", "", "Optionally specify the path to a file containing expression aliases in the form alias=template(%v,%v) with one alias per line")
	EnableGeo       = flag.Bool("enablegeo", false, "enable geolocation functions")
	ISPFormat       = flag.String("ispformat", "ip2location", "ip2location or maxmind")
	ISPDB           = flag.String("ispdb", "", "In order to enable ISP functions, point this to a ISP database file, either in IP2Location Lite format or MaxMind GeoIP2 ISP format")
	RedisAddr       = flag.String("redis", "", "Redis address in \"redis[s]://host:port\" format")
	RedisCA         = flag.String("redisca", "", "Certificate for redislabs's CA")
	RedisClientPK   = flag.String("redisclientpk", "", "Private key for authenticating client to redis's stunnel")
	RedisClientCert = flag.String("redisclientcert", "", "Certificate for authenticating client to redis's stunnel")
	RedisCacheSize  = flag.Int("rediscachesize", 25000, "Configures the maximum size of redis caches for HGET operations, defaults to 25,000 per hash")
	PprofAddr       = flag.String("pprofaddr", "localhost:4000", "if specified, will listen for pprof connections at the specified tcp address")
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

func RedisClient() *redis.Client {
	if *RedisAddr == "" {
		log.Debug("Redis not configured")
		return nil
	}
	log.Debugf("Connecting to Redis at %v", *RedisAddr)
	redisClient, err := tlsredis.GetClient(&tlsredis.Options{
		RedisURL:       *RedisAddr,
		RedisCAFile:    *RedisCA,
		ClientPKFile:   *RedisClientPK,
		ClientCertFile: *RedisClientCert,
	})
	if err == nil {
		log.Debugf("Connected to Redis at %v", *RedisAddr)
		return redisClient
	} else {
		log.Errorf("Unable to connect to redis: %v", err)
		return nil
	}
}
