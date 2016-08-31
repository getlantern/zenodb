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
	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	"github.com/getlantern/appdir"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/rpc"
	"golang.org/x/net/context"
)

const (
	basePrompt  = "zeno-cli >"
	emptyPrompt = "            "
	totalLabel  = "*total*"
)

var (
	log = golog.LoggerFor("zeno-cli")

	addr       = flag.String("addr", ":17712", "The address to which to connect, defaults to localhost:17712")
	queryStats = flag.Bool("querystats", false, "Set this to show query stats on each query")
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
		rl.SetPrompt(emptyPrompt)
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

	if log.IsTraceEnabled() {
		log.Tracef("Query response: %v", spew.Sdump(result))
		_nextRow := nextRow
		nextRow = func() (*zenodb.Row, error) {
			row, err := _nextRow()
			if err != nil {
				log.Tracef("Error fetching row: %v", err)
				return row, err
			}
			log.Tracef("Got row: %v", spew.Sdump(row))
			return row, err
		}
	}

	if csv {
		return dumpCSV(stdout, result, nextRow)
	}
	return dumpPlainText(stdout, sql, result, nextRow)
}

func dumpPlainText(stdout io.Writer, sql string, result *zenodb.QueryResult, nextRow func() (*zenodb.Row, error)) error {
	printQueryStats(os.Stderr, result)

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

	numFields := numFieldsFor(result)

	// Calculate widths for dimensions and fields
	dimWidths := make([]int, len(result.GroupBy))
	fieldWidths := make([]int, numFields)
	totalLabelWidth := len(totalLabel)

	for _, dim := range result.GroupBy {
		dimWidths = append(dimWidths, len(dim))
	}

	for _, fieldName := range result.FieldNames {
		labelWidth := len(fieldName)
		if totalLabelWidth > labelWidth {
			labelWidth = totalLabelWidth
		}
		fieldWidths = append(fieldWidths, labelWidth)
	}

	for _, row := range rows {
		for i, val := range row.Dims {
			width := len(fmt.Sprint(val))
			if width > dimWidths[i] {
				dimWidths[i] = width
			}
		}

		for i := range result.FieldNames {
			var val float64
			if result.IsCrosstab {
				val = row.Totals[i]
			} else {
				val = row.Values[i]
			}
			width := len(fmt.Sprintf("%.4f", val))
			if width > fieldWidths[i] {
				fieldWidths[i] = width
			}
		}

		if result.IsCrosstab {
			outIdx := len(result.FieldNames)
			for i, crosstabDim := range result.CrosstabDims {
				for j, fieldName := range result.FieldNames {
					idx := i*len(result.FieldNames) + j
					if result.PopulatedColumns[idx] {
						val := row.Values[idx]
						width := len(fmt.Sprintf("%.4f", val))
						labelWidth := len(fieldName)
						if labelWidth > width {
							width = labelWidth
						}
						crosstabDimWidth := len(fmt.Sprint(crosstabDim))
						if crosstabDimWidth > width {
							width = crosstabDimWidth
						}
						if width > fieldWidths[outIdx] {
							fieldWidths[outIdx] = width
						}
						outIdx++
					}
				}
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

	if result.IsCrosstab {
		// Print crosstab header row
		fmt.Fprintf(stdout, "# %-33v", "")
		for i := range result.GroupBy {
			fmt.Fprintf(stdout, dimFormats[i], "")
		}

		// Print totals
		outIdx := 0
		for range result.FieldNames {
			fmt.Fprintf(stdout, fieldLabelFormats[outIdx], totalLabel)
			outIdx++
		}
		for i, crosstabDim := range result.CrosstabDims {
			for j := range result.FieldNames {
				idx := i*len(result.FieldNames) + j
				if result.PopulatedColumns[idx] {
					fmt.Fprintf(stdout, fieldLabelFormats[outIdx], crosstabDim)
					outIdx++
				}
			}
		}
		fmt.Fprint(stdout, "\n")
	}

	// Print header row
	fmt.Fprintf(stdout, "# %-33v", "time")
	for i, dim := range result.GroupBy {
		fmt.Fprintf(stdout, dimFormats[i], dim)
	}
	outIdx := 0
	for i, fieldName := range result.FieldNames {
		fmt.Fprintf(stdout, fieldLabelFormats[i], fieldName)
		outIdx++
	}
	if result.IsCrosstab {
		for i := range result.CrosstabDims {
			for j, fieldName := range result.FieldNames {
				idx := i*len(result.FieldNames) + j
				if result.PopulatedColumns[idx] {
					fmt.Fprintf(stdout, fieldLabelFormats[outIdx], fieldName)
					outIdx++
				}
			}
		}
	}
	fmt.Fprint(stdout, "\n")

	for _, row := range rows {
		fmt.Fprintf(stdout, "%-35v", result.Until.Add(-1*time.Duration(row.Period)*result.Resolution).In(time.UTC).Format(time.RFC1123))
		for i, dim := range row.Dims {
			fmt.Fprintf(stdout, dimFormats[i], dim)
		}

		outIdx := 0
		for i := range result.FieldNames {
			var val float64
			if result.IsCrosstab {
				val = row.Totals[i]
			} else {
				val = row.Values[i]
			}
			fmt.Fprintf(stdout, fieldFormats[outIdx], val)
			outIdx++
		}

		for i := range result.CrosstabDims {
			for j := range result.FieldNames {
				idx := i*len(result.FieldNames) + j
				if result.PopulatedColumns[idx] {
					fmt.Fprintf(stdout, fieldFormats[outIdx], row.Values[idx])
					outIdx++
				}
			}
		}

		fmt.Fprint(stdout, "\n")
	}

	return nil
}

func dumpCSV(stdout io.Writer, result *zenodb.QueryResult, nextRow func() (*zenodb.Row, error)) error {
	printQueryStats(os.Stderr, result)

	w := csv.NewWriter(stdout)
	defer w.Flush()

	numFields := numFieldsFor(result)

	// Write header
	rowStrings := make([]string, 0, 1+len(result.GroupBy)+numFields)
	rowStrings = append(rowStrings, "time")
	for _, dim := range result.GroupBy {
		rowStrings = append(rowStrings, dim)
	}
	for i, field := range result.FieldNames {
		if result.IsCrosstab {
			rowStrings = append(rowStrings, field)
			for j := range result.CrosstabDims {
				idx := j*len(result.FieldNames) + i
				if result.PopulatedColumns[idx] {
					rowStrings = append(rowStrings, field)
				}
			}
		} else {
			rowStrings = append(rowStrings, field)
		}
	}
	w.Write(rowStrings)
	w.Flush()

	if result.IsCrosstab {
		// Write 2nd header row
		rowStrings := make([]string, 0, 1+len(result.GroupBy)+numFields)
		rowStrings = append(rowStrings, "")
		for range result.GroupBy {
			rowStrings = append(rowStrings, "")
		}
		for i := range result.FieldNames {
			rowStrings = append(rowStrings, totalLabel)
			for j, crosstabDim := range result.CrosstabDims {
				idx := i*len(result.CrosstabDims) + j
				if result.PopulatedColumns[idx] {
					rowStrings = append(rowStrings, fmt.Sprint(crosstabDim))
				}
			}
		}
		w.Write(rowStrings)
		w.Flush()
	}

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
		rowStrings = append(rowStrings, result.Until.Add(-1*result.Resolution*time.Duration(row.Period)).In(time.UTC).Format(time.RFC3339))
		for _, dim := range row.Dims {
			rowStrings = append(rowStrings, fmt.Sprint(dim))
		}
		for i := range result.FieldNames {
			if !result.IsCrosstab {
				rowStrings = append(rowStrings, fmt.Sprintf("%f", row.Values[i]))
			} else {
				rowStrings = append(rowStrings, fmt.Sprintf("%f", row.Totals[i]))
				for j := range result.CrosstabDims {
					idx := i*len(result.CrosstabDims) + j
					if result.PopulatedColumns[idx] {
						rowStrings = append(rowStrings, fmt.Sprintf("%f", row.Values[idx]))
					}
				}
			}
		}
		w.Write(rowStrings)
		i++
		if i%100 == 0 {
			w.Flush()
		}
	}

	return nil
}

func numFieldsFor(result *zenodb.QueryResult) int {
	numFields := len(result.FieldNames)
	if result.IsCrosstab {
		for _, populated := range result.PopulatedColumns {
			if populated {
				numFields++
			}
		}
	}
	return numFields
}

func printQueryStats(stderr io.Writer, result *zenodb.QueryResult) {
	if !*queryStats {
		return
	}
	fmt.Fprintln(stderr, "-------------------------------------------------")
	fmt.Fprintf(stderr, "# As Of:      %v\n", result.AsOf.In(time.UTC).Format(time.RFC1123))
	fmt.Fprintf(stderr, "# Until:      %v\n", result.Until.In(time.UTC).Format(time.RFC1123))
	fmt.Fprintf(stderr, "# Resolution: %v\n", result.Resolution)
	fmt.Fprintf(stderr, "# Group By:   %v\n\n", strings.Join(result.GroupBy, " "))

	fmt.Fprintf(stderr, "# Query Runtime:  %v\n\n", result.Stats.Runtime)

	fmt.Fprintln(stderr, "# Key Statistics")
	fmt.Fprintf(stderr, "#   Scanned:       %v\n", humanize.Comma(result.Stats.Scanned))
	fmt.Fprintf(stderr, "#   Filter Pass:   %v\n", humanize.Comma(result.Stats.FilterPass))
	fmt.Fprintf(stderr, "#   Read Value:    %v\n", humanize.Comma(result.Stats.ReadValue))
	fmt.Fprintf(stderr, "#   Valid:         %v\n", humanize.Comma(result.Stats.DataValid))
	fmt.Fprintf(stderr, "#   In Time Range: %v\n", humanize.Comma(result.Stats.InTimeRange))
	fmt.Fprintln(stderr, "-------------------------------------------------\n")
}
