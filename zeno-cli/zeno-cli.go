package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/dustin/go-humanize"
	"github.com/getlantern/appdir"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/rpc"
	"golang.org/x/net/context"
)

const (
	basePrompt = "zeno-cli >"
)

var (
	log = golog.LoggerFor("zeno-cli")

	addr = flag.String("addr", ":17712", "The address to which to connect, defaults to localhost:17712")
)

func main() {
	flag.Parse()

	clidir := appdir.General("zeno-cli")
	err := os.MkdirAll(clidir, 0700)
	if err != nil {
		log.Fatalf("Unable to create directory for saving history: %v", err)
	}
	historyFile := filepath.Join(clidir, "history")
	fmt.Fprintf(os.Stderr, "Will save history to %v\n", historyFile)

	client, err := rpc.Dial(*addr)
	if err != nil {
		log.Fatalf("Unable to dial server at %v: %v", *addr, err)
	}
	defer client.Close()

	if flag.NArg() == 1 {
		// Process single command from command-line and then exit
		sql := strings.Trim(flag.Arg(0), ";")
		queryErr := query(os.Stdout, os.Stderr, client, sql, true)
		if queryErr != nil {
			log.Fatal(queryErr)
		}
		return
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 basePrompt + " ",
		HistoryFile:            historyFile,
		DisableAutoSaveHistory: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rl.Close()

	var cmds []string
	for {
		line, err := rl.Readline()
		if err != nil {
			return
		}
		cmds = processLine(rl, client, cmds, line)
	}
}

func processLine(rl *readline.Instance, client rpc.Client, cmds []string, line string) []string {
	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return cmds
	}
	cmds = append(cmds, line)
	if !strings.HasSuffix(line, ";") {
		rl.SetPrompt(basePrompt + ">> ")
		return cmds
	}
	cmd := strings.Join(cmds, "\n")
	rl.SaveHistory(cmd)
	// Strip trailing semicolon
	cmd = cmd[:len(cmd)-1]
	cmds = cmds[:0]
	rl.SetPrompt(basePrompt + " ")

	err := query(rl.Stdout(), rl.Stderr(), client, cmd, false)
	if err != nil {
		fmt.Fprintln(rl.Stderr(), err)
	}

	return cmds
}

func query(stdout io.Writer, stderr io.Writer, client rpc.Client, sql string, csv bool) error {
	result, nextRow, err := client.Query(context.Background(), &rpc.Query{
		SQL: sql,
	})
	if err != nil {
		return err
	}

	if csv {
		return dumpCSV(stdout, result, nextRow)
	} else {
		return dumpPlainText(stdout, sql, result, nextRow)
	}
}

func dumpPlainText(stdout io.Writer, sql string, result *zenodb.QueryResult, nextRow func() (*zenodb.Row, error)) error {
	// Read all rows into list
	var rows []*zenodb.Row
	for {
		row, err := nextRow()
		if err == io.EOF {
			// Done
			break
		}
		if err != nil {
			return fmt.Errorf("Unable to get next row: %v\n", err)
		}
		rows = append(rows, row)
	}

	// Print summary info
	fmt.Fprintln(stdout, "-------------------------------------------------")
	fmt.Fprintf(stdout, "# As Of:      %v\n", result.AsOf)
	fmt.Fprintf(stdout, "# Until:      %v\n", result.Until)
	fmt.Fprintf(stdout, "# Resolution: %v\n", result.Resolution)
	fmt.Fprintf(stdout, "# Group By:   %v\n\n", strings.Join(result.GroupBy, " "))

	fmt.Fprintf(stdout, "# Query Runtime:  %v\n\n", result.Stats.Runtime)

	fmt.Fprintln(stdout, "# Key Statistics")
	fmt.Fprintf(stdout, "#   Scanned:       %v\n", humanize.Comma(result.Stats.Scanned))
	fmt.Fprintf(stdout, "#   Filter Pass:   %v\n", humanize.Comma(result.Stats.FilterPass))
	fmt.Fprintf(stdout, "#   Read Value:    %v\n", humanize.Comma(result.Stats.ReadValue))
	fmt.Fprintf(stdout, "#   Valid:         %v\n", humanize.Comma(result.Stats.DataValid))
	fmt.Fprintf(stdout, "#   In Time Range: %v\n", humanize.Comma(result.Stats.InTimeRange))
	fmt.Fprintln(stdout, "-------------------------------------------------\n")

	// Calculate widths for dimensions and fields
	dimWidths := make([]int, len(result.GroupBy))
	fieldWidths := make([]int, len(result.FieldNames))

	for i, dim := range result.GroupBy {
		width := len(dim)
		if width > dimWidths[i] {
			dimWidths[i] = width
		}
	}

	for i, field := range result.FieldNames {
		width := len(field)
		if width > fieldWidths[i] {
			fieldWidths[i] = width
		}
	}

	for _, row := range rows {
		for i, val := range row.Dims {
			width := len(fmt.Sprint(val))
			if width > dimWidths[i] {
				dimWidths[i] = width
			}
		}

		for i, val := range row.Values {
			width := len(fmt.Sprintf("%.4f", val))
			if width > fieldWidths[i] {
				fieldWidths[i] = width
			}
		}
	}

	// Create formats for dims and fields
	dimFormats := make([]string, 0, len(dimWidths))
	fieldLabelFormats := make([]string, 0, len(fieldWidths))
	fieldFormats := make([]string, 0, len(fieldWidths))
	for _, width := range dimWidths {
		dimFormats = append(dimFormats, "%-"+fmt.Sprint(width+4)+"v")
	}
	for _, width := range fieldWidths {
		fieldLabelFormats = append(fieldLabelFormats, "%"+fmt.Sprint(width+4)+"v")
		fieldFormats = append(fieldFormats, "%"+fmt.Sprint(width+4)+".4f")
	}

	// Print header row
	fmt.Fprintf(stdout, "# %-33v", "time")
	for i, dim := range result.GroupBy {
		fmt.Fprintf(stdout, dimFormats[i], dim)
	}
	for i, field := range result.FieldNames {
		fmt.Fprintf(stdout, fieldLabelFormats[i], field)
	}
	fmt.Fprint(stdout, "\n")

	for _, row := range rows {
		fmt.Fprintf(stdout, "%-35v", result.Until.Add(-1*time.Duration(row.Period)*result.Resolution).Format(time.RFC1123))
		for i, dim := range row.Dims {
			fmt.Fprintf(stdout, dimFormats[i], dim)
		}
		for i, val := range row.Values {
			fmt.Fprintf(stdout, fieldFormats[i], val)
		}
		fmt.Fprint(stdout, "\n")
	}

	return nil
}

func dumpCSV(stdout io.Writer, result *zenodb.QueryResult, nextRow func() (*zenodb.Row, error)) error {
	w := csv.NewWriter(stdout)
	defer w.Flush()

	rowStrings := make([]string, 0, 1+len(result.GroupBy)+len(result.FieldNames))
	rowStrings = append(rowStrings, "time")
	for _, dim := range result.GroupBy {
		rowStrings = append(rowStrings, dim)
	}
	for _, field := range result.FieldNames {
		rowStrings = append(rowStrings, field)
	}
	w.Write(rowStrings)
	w.Flush()

	i := 0
	for {
		row, err := nextRow()
		if err == io.EOF {
			// Done
			break
		}
		if err != nil {
			return fmt.Errorf("Unable to get next row: %v\n", err)
		}
		rowStrings := make([]string, 0, 1+len(result.GroupBy)+len(result.FieldNames))
		rowStrings = append(rowStrings, result.Until.Add(-1*result.Resolution*time.Duration(row.Period)).Format(time.RFC3339))
		for _, dim := range row.Dims {
			rowStrings = append(rowStrings, fmt.Sprint(dim))
		}
		for _, field := range row.Values {
			rowStrings = append(rowStrings, fmt.Sprint(field))
		}
		w.Write(rowStrings)
		i++
		if i%100 == 0 {
			w.Flush()
		}
	}

	return nil
}
