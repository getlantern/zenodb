// zeno is the executable for the ZenoDB database, and can run as a standalone
// server, as a cluster leader or as a cluster follower.
package main

import (
	"flag"
	"time"

	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/cmd"
	"github.com/getlantern/zenodb/server"
	"github.com/vharitonsky/iniflags"
)

var (
	log = golog.LoggerFor("zeno")
)

func main() {
	srv := &server.Server{}
	flag.StringVar(&srv.DBDir, "dbdir", "zenodata", "The directory in which to store the database files, defaults to ./zenodata")
	flag.BoolVar(&srv.Vtime, "vtime", false, "Set this flag to use virtual instead of real time. When using virtual time, the advancement of time will be governed by the timestamps received via inserts.")
	flag.DurationVar(&srv.WALSync, "walsync", 5*time.Second, "How frequently to sync the WAL to disk. Set to 0 to sync after every write. Defaults to 5 seconds.")
	flag.IntVar(&srv.MaxWALSize, "maxwalsize", 1024*1024*1024, "Maximum size of WAL segments on disk. Defaults to 1 GB.")
	flag.IntVar(&srv.WALCompressionSize, "walcompressionsize", 30*1024*1024, "Size above which to start compressing WAL segments with snappy. Defaults to 30 MB.")
	flag.Float64Var(&srv.MaxMemory, "maxmemory", 0.7, "Set to a non-zero value to cap the total size of the process as a percentage of total system memory. Defaults to 0.7 = 70%.")
	flag.DurationVar(&srv.IterationCoalesceInterval, "itercoalesce", zenodb.DefaultIterationCoalesceInterval, "Period to wait for coalescing parallel iterations")
	flag.IntVar(&srv.IterationConcurrency, "iterconcurrency", zenodb.DefaultIterationConcurrency, "specifies the maximum concurrency for iterating tables")
	flag.StringVar(&srv.Addr, "addr", "localhost:17712", "The address at which to listen for gRPC over TLS connections, defaults to localhost:17712")
	flag.StringVar(&srv.HTTPSAddr, "httpsaddr", "localhost:17713", "The address at which to listen for JSON over HTTPS connections, defaults to localhost:17713")
	flag.StringVar(&srv.Password, "password", "", "if specified, will authenticate clients using this password")
	flag.StringVar(&srv.PKFile, "pkfile", "pk.pem", "path to the private key PEM file")
	flag.StringVar(&srv.CertFile, "certfile", "cert.pem", "path to the certificate PEM file")
	flag.StringVar(&srv.CookieHashKey, "cookiehashkey", "", "key to use for HMAC authentication of web auth cookies, should be 64 bytes, defaults to random 64 bytes if not specified")
	flag.StringVar(&srv.CookieBlockKey, "cookieblockkey", "", "key to use for encrypting web auth cookies, should be 32 bytes, defaults to random 32 bytes if not specified")
	flag.StringVar(&srv.OauthClientID, "oauthclientid", "", "id to use for oauth client to connect to GitHub")
	flag.StringVar(&srv.OauthClientSecret, "oauthclientsecret", "", "secret id to use for oauth client to connect to GitHub")
	flag.StringVar(&srv.GitHubOrg, "githuborg", "", "the GitHug org against which web users are authenticated")
	flag.BoolVar(&srv.Insecure, "insecure", false, "set to true to disable TLS certificate verification when connecting to other zeno servers (don't use this in production!)")
	flag.BoolVar(&srv.Passthrough, "passthrough", false, "set to true to make this node a passthrough that doesn't capture data in table but is capable of feeding and querying other nodes. requires that -partitions be specified.")
	flag.StringVar(&srv.Capture, "capture", "", "if specified, connect to the node at the given address to receive updates, authenticating with value of -password.  requires that you specify which -partition this node handles.")
	flag.StringVar(&srv.CaptureOverride, "captureoverride", "", "if specified, dial network connection for -capture using this address, but verify TLS connection using the address from -capture")
	flag.StringVar(&srv.Feed, "feed", "", "if specified, connect to the nodes at the given comma,delimited addresses to handle queries for them, authenticating with value of -password. requires that you specify which -partition this node handles.")
	flag.StringVar(&srv.FeedOverride, "feedoverride", "", "if specified, dial network connection for -feed using this address, but verify TLS connection using the address from -feed")
	flag.IntVar(&srv.ID, "id", 0, "unique identifier for a leader")
	flag.IntVar(&srv.NumPartitions, "numpartitions", 1, "The number of partitions available to distribute amongst followers")
	flag.IntVar(&srv.Partition, "partition", 0, "the partition number assigned to this follower")
	flag.IntVar(&srv.ClusterQueryConcurrency, "clusterqueryconcurrency", server.DefaultClusterQueryConcurrency, "specifies the maximum concurrency for clustered queries")
	flag.DurationVar(&srv.ClusterQueryTimeout, "clusterquerytimeout", zenodb.DefaultClusterQueryTimeout, "specifies the maximum time leader will wait for followers to answer a query")
	flag.DurationVar(&srv.NextQueryTimeout, "nextquerytimeout", server.DefaultNextQueryTimeout, "specifies the maximum time follower will wait for leader to send a query on an open connection")
	flag.DurationVar(&srv.MaxFollowAge, "maxfollowage", 0, "user with -follow, limits how far to go back when pulling data from leader")
	flag.StringVar(&srv.TLSDomain, "tlsdomain", "", "Specify this to automatically use LetsEncrypt certs for this domain")
	flag.DurationVar(&srv.WebQueryCacheTTL, "webquerycachettl", 2*time.Hour, "specifies how long to cache web query results")
	flag.DurationVar(&srv.WebQueryTimeout, "webquerytimeout", 30*time.Minute, "time out web queries after this duration")
	flag.IntVar(&srv.WebQueryConcurrencyLimit, "webqueryconcurrency", 2, "limit concurrent web queries to this (subsequent queries will be queued)")
	flag.IntVar(&srv.WebMaxResponseBytes, "webquerymaxresponsebytes", 25*1024*1024, "limit the size of query results returned through the web API")
	flag.DurationVar(&srv.RPCKeepaliveInterval, "rpckeealiveinterval", 10*time.Second, "interval at which to ping leader via RPC")
	flag.DurationVar(&srv.RPCKeepAliveTimeout, "rpckeepalivetimeout", 5*time.Second, "time to wait for ping response from leader before reconnecting")

	iniflags.Parse()

	srv.Schema = *cmd.Schema
	srv.AliasesFile = *cmd.AliasesFile
	srv.EnableGeo = *cmd.EnableGeo
	srv.RedisCacheSize = *cmd.RedisCacheSize

	cmd.StartPprof()

	srv.HandleShutdownSignal()
	wait, err := srv.Serve()
	if err != nil {
		log.Fatal(err)
	}
	if err := wait(); err != nil {
		log.Fatal(err)
	}
	log.Debug("Done")
}
