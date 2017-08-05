package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"

	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb/cmd"
	"github.com/vharitonsky/iniflags"
)

var (
	log = golog.LoggerFor("zenotool")

	outfile = flag.String("o", "", "Name of file to which to write output")
	filter  = flag.String("filter", "", "SQL WHERE clause for filtering rows")
)

func main() {
	iniflags.Parse()

	if *outfile == "" {
		log.Fatal("Please specify an output file using -out")
	}

	if *cmd.PprofAddr != "" {
		go func() {
			log.Debugf("Starting pprof page at http://%s/debug/pprof", *cmd.PprofAddr)
			if err := http.ListenAndServe(*cmd.PprofAddr, nil); err != nil {
				log.Error(err)
			}
		}()
	}

	infiles := flag.Args()
	log.Debug(infiles)
}
