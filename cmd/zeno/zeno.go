// zeno is the executable for the ZenoDB database, and can run as a standalone
// server, as a cluster leader or as a cluster follower.
package main

import (
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/cmd"
	"github.com/getlantern/zenodb/server"
	"github.com/vharitonsky/iniflags"
)

var (
	log = golog.LoggerFor("zeno")
)

func main() {
	srv := &server.Server{}
	srv.ConfigureFlags()
	iniflags.SetAllowUnknownFlags(true)
	iniflags.Parse()

	srv.Schema = *cmd.Schema
	srv.AliasesFile = *cmd.AliasesFile
	srv.EnableGeo = *cmd.EnableGeo
	srv.RedisCacheSize = *cmd.RedisCacheSize

	cmd.StartPprof()

	srv.HandleShutdownSignal()
	_, run, err := srv.Prepare()
	if err != nil {
		log.Fatal(err)
	}
	if err := run(); err != nil {
		log.Fatal(err)
	}
	log.Debug("Done")
}
