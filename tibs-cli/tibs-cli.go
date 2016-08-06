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
	"github.com/getlantern/appdir"
	"github.com/getlantern/golog"
	"github.com/getlantern/tibsdb/rpc"
	"golang.org/x/net/context"
)

var (
	log = golog.LoggerFor("tibsdb-cli")

	addr = flag.String("addr", ":17712", "The address to which to connect, defaults to localhost:17712")
)

func main() {
	flag.Parse()

	clidir := appdir.General("tibsdb-cli")
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
		queryErr := query(os.Stdout, os.Stderr, client, sql)
		if queryErr != nil {
			log.Fatal(queryErr)
		}
		return
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 "> ",
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
		rl.SetPrompt(">>> ")
		return cmds
	}
	cmd := strings.Join(cmds, "\n")
	rl.SaveHistory(cmd)
	// Strip trailing semicolon
	cmd = cmd[:len(cmd)-1]
	cmds = cmds[:0]
	rl.SetPrompt("> ")

	err := query(rl.Stdout(), rl.Stderr(), client, cmd)
	if err != nil {
		fmt.Fprintln(rl.Stderr(), err)
	}

	return cmds
}

func query(stdout io.Writer, stderr io.Writer, client rpc.Client, sql string) error {
	result, nextRow, err := client.Query(context.Background(), &rpc.Query{
		SQL: sql,
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(stderr, "%v -> %v\n", result.AsOf.Format(time.RFC1123), result.Until.Format(time.RFC1123))
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
			return fmt.Errorf("Unable to stream response: %v\n", err)
		}
		rowStrings := make([]string, 0, 1+len(result.GroupBy)+len(result.FieldNames))
		rowStrings = append(rowStrings, result.Until.Add(-1*result.Resolution*time.Duration(row.Period)).Format(time.RFC1123))
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
