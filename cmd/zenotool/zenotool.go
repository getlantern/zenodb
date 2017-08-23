// zenotool provides the ability to filter and merge zeno datafiles offline.
package main

import (
	"flag"
	"strings"

	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/cmd"
	"github.com/vharitonsky/iniflags"
)

var (
	log = golog.LoggerFor("zenotool")

	table      = flag.String("table", "", "Name of table corresponding to these files")
	outFile    = flag.String("out", "", "Name of file to which to write output")
	where      = flag.String("where", "", "SQL WHERE clause for filtering rows")
	shouldSort = flag.Bool("sort", false, "Sort the output")
)

func main() {
	iniflags.SetAllowUnknownFlags(true)
	iniflags.Parse()

	if *table == "" {
		log.Fatal("Please specify a table using -table")
	}

	if *outFile == "" {
		log.Fatal("Please specify an output file using -out")
	}

	cmd.StartPprof()

	db, err := zenodb.NewDB(&zenodb.DBOpts{
		SchemaFile:     *cmd.Schema,
		EnableGeo:      *cmd.EnableGeo,
		ISPProvider:    cmd.ISPProvider(),
		AliasesFile:    *cmd.AliasesFile,
		RedisClient:    cmd.RedisClient(),
		RedisCacheSize: *cmd.RedisCacheSize,
	})
	if err != nil {
		log.Fatalf("Unable to initialize DB: %v", err)
	}

	inFiles := flag.Args()
	err = db.FilterAndMerge(*table, *where, *shouldSort, *outFile, inFiles...)
	if err != nil {
		log.Fatalf("Unable to perform merge: %v", err)
	}

	log.Debugf("Merged %v -> %v", strings.Join(inFiles, " + "), *outFile)
}
