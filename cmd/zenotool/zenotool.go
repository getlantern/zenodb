// zenotool provides the ability to filter and merge zeno datafiles offline.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/cmd"
	"github.com/getlantern/zenodb/web"
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
	checktable = flag.Bool("checktable", false, "If set, this checks a single datafile for a given table")
	permalinks = flag.Bool("permalinks", false, "If set, this returns a list of the permalinks in the database's webcache")
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
			offsetsBySource, fieldsString, _, err := zenodb.FileInfo(inFile)
			if err != nil {
				log.Error(err)
			} else {
				log.Debugf("%v   highWaterMarks: %v    fields: %v", inFile, offsetsBySource.TSString(), fieldsString)
			}
		}
		return
	}

	if *permalinks {
		fmt.Fprintln(os.Stderr, "Listing permalinks")
		out := csv.NewWriter(os.Stdout)
		out.Write([]string{"Date", "Rows", "Permalink", "SQL"})
		for _, cacheFile := range inFiles {
			permalinks, err := web.ListPermalinks(cacheFile)
			if err != nil {
				log.Error(err)
			} else {
				for _, permalink := range permalinks {
					out.Write([]string{permalink.Timestamp.Format("Jan 02 2006"), strconv.Itoa(permalink.NumRows), permalink.Permalink, permalink.SQL})
				}
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

	if *checktable {
		if err := db.CheckTable(*table, inFiles[0]); err != nil {
			log.Fatal(err)
		}
		return
	}

	if *outFile == "" {
		log.Fatal("Please specify an output file using -out")
	}

	err = db.FilterAndMerge(*table, *where, *shouldSort, *outFile, inFiles...)
	if err != nil {
		log.Fatalf("Unable to perform merge: %v", err)
	}

	log.Debugf("Merged %v -> %v", strings.Join(inFiles, " + "), *outFile)
}
