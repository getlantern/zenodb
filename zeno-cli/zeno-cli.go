package main

import (
	"crypto/tls"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/davecgh/go-spew/spew"
	"github.com/getlantern/appdir"
	"github.com/getlantern/golog"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
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

	addr       = flag.String("addr", ":17712", "The address to which to connect with gRPC over TLS, defaults to localhost:17712")
	insecure   = flag.Bool("insecure", false, "set to true to disable TLS certificate verification when connecting to the server (don't use this in production!)")
	fresh      = flag.Bool("fresh", false, "Set this flag to include data not yet flushed from memstore in query results")
	queryStats = flag.Bool("querystats", false, "Set this to show query stats on each query")
	password   = flag.String("password", "", "if specified, will authenticate against server using this password")
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

	host, _, _ := net.SplitHostPort(*addr)
	tlsConfig := &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: *insecure,
		ClientSessionCache: tls.NewLRUClientSessionCache(100),
	}

	client, err := rpc.Dial(*addr, &rpc.ClientOpts{
		Password: *password,
		Dialer: func(addr string, timeout time.Duration) (net.Conn, error) {
			conn, dialErr := net.DialTimeout("tcp", addr, timeout)
			if dialErr != nil {
				return nil, dialErr
			}
			tlsConn := tls.Client(conn, tlsConfig)
			return tlsConn, tlsConn.Handshake()
		},
	})
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
	md, iterate, err := client.Query(context.Background(), sql, *fresh)
	if err != nil {
		return err
	}

	if csv {
		return dumpCSV(stdout, md, iterate)
	}
	return dumpPlainText(stdout, sql, md, iterate)
}

func dumpPlainText(stdout io.Writer, sql string, md *zenodb.QueryMetaData, iterate func(onRow core.OnFlatRow) error) error {
	printQueryStats(os.Stderr, md)

	// Read all rows into list and collect unique dimensions
	var rows []*core.FlatRow
	uniqueDims := make(map[string]bool)
	err := iterate(func(row *core.FlatRow) (bool, error) {
		if log.IsTraceEnabled() {
			log.Tracef("Got row: %v", spew.Sdump(row))
		}
		rows = append(rows, row)
		for k := range row.Key.AsMap() {
			uniqueDims[k] = true
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	numFields := numFieldsFor(md)

	// Calculate widths for dimensions and fields
	dimWidths := make([]int, 0, len(uniqueDims))
	fieldWidths := make([]int, 0, numFields)
	totalLabelWidth := len(totalLabel)

	groupBy := make([]string, 0, len(uniqueDims))
	for dim := range uniqueDims {
		groupBy = append(groupBy, dim)
	}
	sort.Strings(groupBy)

	for _, dim := range groupBy {
		dimWidths = append(dimWidths, len(dim))
	}

	for _, fieldName := range md.FieldNames {
		labelWidth := len(fieldName)
		if totalLabelWidth > labelWidth {
			labelWidth = totalLabelWidth
		}
		fieldWidths = append(fieldWidths, labelWidth)
	}

	for _, row := range rows {
		for i, dim := range groupBy {
			val := row.Key.Get(dim)
			width := len(fmt.Sprint(val))
			if width > dimWidths[i] {
				dimWidths[i] = width
			}
		}

		for i := range md.FieldNames {
			var val float64
			// if result.IsCrosstab {
			// 	val = row.Totals[i]
			// } else {
			val = row.Values[i]
			// }
			width := len(fmt.Sprintf("%.4f", val))
			if width > fieldWidths[i] {
				fieldWidths[i] = width
			}
		}

		// TODO: enable crosstab support by cross tabbing in client (or maybe in bytetree?)
		// if result.IsCrosstab {
		// 	outIdx := len(result.FieldNames)
		// 	for i, crosstabDim := range result.CrosstabDims {
		// 		for j, fieldName := range result.FieldNames {
		// 			idx := i*len(result.FieldNames) + j
		// 			if result.PopulatedColumns[idx] {
		// 				val := row.Values[idx]
		// 				width := len(fmt.Sprintf("%.4f", val))
		// 				labelWidth := len(fieldName)
		// 				if labelWidth > width {
		// 					width = labelWidth
		// 				}
		// 				crosstabDimWidth := len(fmt.Sprint(nilToDash(crosstabDim)))
		// 				if crosstabDimWidth > width {
		// 					width = crosstabDimWidth
		// 				}
		// 				if len(fieldWidths) <= outIdx {
		// 					fieldWidths = append(fieldWidths, width)
		// 				} else if width > fieldWidths[outIdx] {
		// 					fieldWidths[outIdx] = width
		// 				}
		// 				outIdx++
		// 			}
		// 		}
		// 	}
		// }
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

	// if result.IsCrosstab {
	// 	// Print crosstab header row
	// 	fmt.Fprintf(stdout, "# %-33v", "")
	// 	for i := range result.GroupBy {
	// 		fmt.Fprintf(stdout, dimFormats[i], "")
	// 	}
	//
	// 	// Print totals
	// 	outIdx := 0
	// 	for range result.FieldNames {
	// 		fmt.Fprintf(stdout, fieldLabelFormats[outIdx], totalLabel)
	// 		outIdx++
	// 	}
	// 	for i, crosstabDim := range result.CrosstabDims {
	// 		for j := range result.FieldNames {
	// 			idx := i*len(result.FieldNames) + j
	// 			if result.PopulatedColumns[idx] {
	// 				fmt.Fprintf(stdout, fieldLabelFormats[outIdx], nilToDash(crosstabDim))
	// 				outIdx++
	// 			}
	// 		}
	// 	}
	// 	fmt.Fprint(stdout, "\n")
	// }

	// Print header row
	fmt.Fprintf(stdout, "# %-33v", "time")
	for i, dim := range groupBy {
		fmt.Fprintf(stdout, dimFormats[i], nilToDash(dim))
	}
	outIdx := 0
	for i, fieldName := range md.FieldNames {
		fmt.Fprintf(stdout, fieldLabelFormats[i], fieldName)
		outIdx++
	}
	// if result.IsCrosstab {
	// 	for i := range result.CrosstabDims {
	// 		for j, fieldName := range result.FieldNames {
	// 			idx := i*len(result.FieldNames) + j
	// 			if result.PopulatedColumns[idx] {
	// 				fmt.Fprintf(stdout, fieldLabelFormats[outIdx], fieldName)
	// 				outIdx++
	// 			}
	// 		}
	// 	}
	// }
	fmt.Fprint(stdout, "\n")

	for _, row := range rows {
		fmt.Fprintf(stdout, "%-35v", encoding.TimeFromInt(row.TS).In(time.UTC).Format(time.RFC1123))
		for i, dim := range groupBy {
			val := row.Key.Get(dim)
			fmt.Fprintf(stdout, dimFormats[i], nilToDash(val))
		}

		outIdx := 0
		for i := range md.FieldNames {
			var val float64
			// if result.IsCrosstab {
			// 	val = row.Totals[i]
			// } else {
			val = row.Values[i]
			// }
			fmt.Fprintf(stdout, fieldFormats[outIdx], val)
			outIdx++
		}

		// for i := range result.CrosstabDims {
		// 	for j := range result.FieldNames {
		// 		idx := i*len(result.FieldNames) + j
		// 		if result.PopulatedColumns[idx] {
		// 			fmt.Fprintf(stdout, fieldFormats[outIdx], row.Values[idx])
		// 			outIdx++
		// 		}
		// 	}
		// }

		fmt.Fprint(stdout, "\n")
	}

	return nil
}

func dumpCSV(stdout io.Writer, md *zenodb.QueryMetaData, iterate func(onRow core.OnFlatRow) error) error {
	printQueryStats(os.Stderr, md)

	w := csv.NewWriter(stdout)
	defer w.Flush()

	numFields := numFieldsFor(md)

	i := 0
	var knownDims []string
	err := iterate(func(row *core.FlatRow) (bool, error) {
		dims := row.Key.AsMap()
		rowStrings := make([]string, 0, 1+len(dims)+len(md.FieldNames))
		rowStrings = append(rowStrings, encoding.TimeFromInt(row.TS).In(time.UTC).Format(time.RFC3339))
		for i := range md.FieldNames {
			var value float64
			// if result.IsCrosstab {
			// 	value = row.Totals[i]
			// } else {
			value = row.Values[i]
			// }
			rowStrings = append(rowStrings, fmt.Sprintf("%f", value))
		}
		// First add known dims
		for _, dim := range knownDims {
			val := dims[dim]
			rowStrings = append(rowStrings, fmt.Sprint(nilToBlank(val)))
			delete(dims, dim)
		}
		// Then add new dims
		if len(dims) > 0 {
			// Alphabetize remaining dims
			dimNames := make([]string, 0, len(dims))
			for dim := range dims {
				dimNames = append(dimNames, dim)
			}
			sort.Strings(dimNames)
			for _, dim := range dimNames {
				val := dims[dim]
				rowStrings = append(rowStrings, fmt.Sprint(nilToBlank(val)))
				rowStrings = append(rowStrings, fmt.Sprint(nilToBlank(val)))
				knownDims = append(knownDims, dim)
			}
		}
		// if result.IsCrosstab {
		// 	for i := range result.CrosstabDims {
		// 		for j := range result.FieldNames {
		// 			idx := i*len(result.FieldNames) + j
		// 			if result.PopulatedColumns[idx] {
		// 				rowStrings = append(rowStrings, fmt.Sprintf("%f", row.Values[idx]))
		// 			}
		// 		}
		// 	}
		// }
		w.Write(rowStrings)
		i++
		if i%100 == 0 {
			w.Flush()
		}
		return true, nil
	})

	if err != nil {
		return err
	}

	// Write header
	rowStrings := make([]string, 0, 1+numFields)
	rowStrings = append(rowStrings, "time")
	for _, fieldName := range md.FieldNames {
		rowStrings = append(rowStrings, fieldName)
	}
	for _, dim := range knownDims {
		rowStrings = append(rowStrings, dim)
	}
	// if result.IsCrosstab {
	// 	for i := range result.CrosstabDims {
	// 		for j, fieldName := range result.FieldNames {
	// 			idx := i*len(result.FieldNames) + j
	// 			if result.PopulatedColumns[idx] {
	// 				rowStrings = append(rowStrings, fieldName)
	// 			}
	// 		}
	// 	}
	// }

	// if result.IsCrosstab {
	// 	// Write crosstab dimensions
	// 	rowStrings := make([]string, 0, 1+len(result.GroupBy)+numFields)
	// 	rowStrings = append(rowStrings, "")
	// 	for range result.GroupBy {
	// 		rowStrings = append(rowStrings, "")
	// 	}
	// 	// Totals
	// 	for range result.FieldNames {
	// 		rowStrings = append(rowStrings, totalLabel)
	// 	}
	// 	if result.IsCrosstab {
	// 		// Per crosstab dimension
	// 		for i, crosstabDim := range result.CrosstabDims {
	// 			for j := range result.FieldNames {
	// 				idx := i*len(result.FieldNames) + j
	// 				if result.PopulatedColumns[idx] {
	// 					rowStrings = append(rowStrings, fmt.Sprint(nilToBlank(crosstabDim)))
	// 				}
	// 			}
	// 		}
	// 	}
	// 	w.Write(rowStrings)
	// 	w.Flush()
	// }

	w.Write(rowStrings)
	w.Flush()
	return nil
}

func nilToBlank(val interface{}) interface{} {
	if val == nil {
		return ""
	}
	return val
}

func nilToDash(val interface{}) interface{} {
	if val == nil {
		return "-----"
	}
	return val
}

func numFieldsFor(md *zenodb.QueryMetaData) int {
	numFields := len(md.FieldNames)
	// if result.IsCrosstab {
	// 	for _, populated := range result.PopulatedColumns {
	// 		if populated {
	// 			numFields++
	// 		}
	// 	}
	// }
	return numFields
}

func printQueryStats(stderr io.Writer, md *zenodb.QueryMetaData) {
	// TODO: maybe restore additional stats?
	if !*queryStats {
		return
	}
	fmt.Fprintln(stderr, "-------------------------------------------------")
	fmt.Fprintf(stderr, "# As Of:      %v\n", md.AsOf.In(time.UTC).Format(time.RFC1123))
	fmt.Fprintf(stderr, "# Until:      %v\n", md.Until.In(time.UTC).Format(time.RFC1123))
	fmt.Fprintf(stderr, "# Resolution: %v\n", md.Resolution)
	fmt.Fprintln(stderr, "-------------------------------------------------\n")
}
