package cmd

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"

	"strings"

	"github.com/getlantern/goexpr/isp"
	"github.com/getlantern/goexpr/isp/ip2location"
	"github.com/getlantern/goexpr/isp/maxmind"
	"github.com/getlantern/golog"
	"github.com/getlantern/keyman"
	rclient "github.com/go-redis/redis/v8"
)

var (
	log            = golog.LoggerFor("cmd")
	redisURLRegExp = regexp.MustCompile(`^rediss\+sentinel://.*?:(.*?)@([\d\.(:\d*)?,]*)$`)
)

var (
	Schema          = flag.String("schema", "schema.yaml", "Location of schema file, defaults to ./schema.yaml")
	AliasesFile     = flag.String("aliases", "", "Optionally specify the path to a file containing expression aliases in the form alias=template(%v,%v) with one alias per line")
	EnableGeo       = flag.Bool("enablegeo", false, "enable geolocation functions")
	ISPFormat       = flag.String("ispformat", "ip2location", "ip2location or maxmind")
	ISPDB           = flag.String("ispdb", "", "In order to enable ISP functions, point this to a ISP database file, either in IP2Location Lite format or MaxMind GeoIP2 ISP format")
	RedisAddr       = flag.String("redis", "", "Redis address in \"redis[s][+sentinel]://host:port[@sentinel-ip1,sentinel-ip2]\" format")
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
func parseRedisURL(redisURL string) (password string, hosts []string, err error) {
	matches := redisURLRegExp.FindStringSubmatch(redisURL)
	if len(matches) < 3 {
		return "", nil, fmt.Errorf("should match %s", redisURLRegExp.String())
	}
	return matches[1], strings.Split(matches[2], ","), nil
}
func RedisClient() *rclient.Client {
	if *RedisAddr == "" {
		log.Error("Redis not configured")
		return nil
	}
	if _, err := os.Stat(*RedisCA); os.IsNotExist(err) {
		log.Fatalf("Cannot find certificate authority file: %v", *RedisCA)
	}
	if _, err := os.Stat(*RedisClientPK); os.IsNotExist(err) {
		log.Fatalf("Cannot find client private key file: %v", *RedisClientPK)
	}
	if _, err := os.Stat(*RedisClientCert); os.IsNotExist(err) {
		log.Fatalf("Cannot find client certificate file: %v", *RedisClientCert)
	}
	redisClientCert, err := tls.LoadX509KeyPair(*RedisClientCert, *RedisClientPK)
	if err != nil {
		log.Fatalf("Failed to load client certificate: %v", err)
	}
	redisCACert, err := keyman.LoadCertificateFromFile(*RedisCA)
	if err != nil {
		log.Fatalf("Failed to load CA cert: %v", err)
	}

	redisPassword, redisHosts, err := parseRedisURL(*RedisAddr)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}

	log.Debugf("Connecting to Redis at %v", *RedisAddr)
	redisOpts := rclient.FailoverOptions{
		SentinelAddrs:    redisHosts,
		SentinelPassword: redisPassword,
		Password:         redisPassword,
		MasterName:       "mymaster",
		// We use TLS as the transport. If we simply specify the TLSConfig, the Redis library will
		// establish TLS connections to Sentinel, but plain TCP connections to masters.
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return tls.Dial(network, addr, &tls.Config{
				RootCAs:            redisCACert.PoolContainingCert(),
				Certificates:       []tls.Certificate{redisClientCert},
				ClientSessionCache: tls.NewLRUClientSessionCache(100),
			})
		},
	}
	c := rclient.NewFailoverClient(&redisOpts)
	if err := c.Ping(context.Background()).Err(); err != nil {
		log.Errorf("error pinging redis: %v", err)
		return nil
	} else {
		log.Debugf("Connected to Redis at %v", *RedisAddr)
		return c

	}
}
