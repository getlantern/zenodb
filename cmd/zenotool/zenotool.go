// zenotool provides the ability to filter and merge zeno datafiles offline.
package main

import (
	"flag"
	"os"
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
	info       = flag.Bool("info", false, "If set, this simply shows information about the input files, no schema required")
	check      = flag.Bool("check", false, "If set, this scans the files and makes sure they're fully readable")
)

func main() {
	iniflags.SetAllowUnknownFlags(true)
	iniflags.Parse()

	inFiles := flag.Args()
	if len(inFiles) == 0 {
		log.Fatal("Please specify at last one input file")
	}

	if *info {
		for _, inFile := range inFiles {
			highWaterMark, fieldsString, _, err := zenodb.FileInfo(inFile)
			if err != nil {
				log.Error(err)
			} else {
				log.Debugf("%v   highWaterMark: %v    fields: %v", inFile, highWaterMark, fieldsString)
			}
		}
		return
	}

	if *check {
		errors := zenodb.Check(inFiles...)
		if len(errors) > 0 {
			log.Debug("------------- Files with Error -------------")
			for filename, err := range errors {
				log.Debugf("%v     %v", filename, err)
			}
			os.Exit(100)
		}
		log.Debug("All Files Passed Check")
		return
	}

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

	err = db.FilterAndMerge(*table, *where, *shouldSort, *outFile, inFiles...)
	if err != nil {
		log.Fatalf("Unable to perform merge: %v", err)
	}

	log.Debugf("Merged %v -> %v", strings.Join(inFiles, " + "), *outFile)
}
