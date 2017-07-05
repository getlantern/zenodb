package cmd

import (
	"flag"
)

var (
	DBDir           = flag.String("dbdir", "zenodata", "The directory in which to store the database files, defaults to ./zenodata")
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
)
