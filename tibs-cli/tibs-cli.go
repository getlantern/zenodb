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
	fmt.Printf("Will save history to %v\n", historyFile)

	client, err := rpc.Dial(*addr)
	if err != nil {
		log.Fatalf("Unable to dial server at %v: %v", *addr, err)
	}
	defer client.Close()

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

	result, nextRow, err := client.Query(context.Background(), &rpc.Query{
		SQL: cmd,
	})
	if err != nil {
		fmt.Fprintln(rl, err.Error())
		return cmds
	}

	fmt.Fprintf(rl, "%v -> %v\n", result.AsOf.Format(time.RFC1123), result.Until.Format(time.RFC1123))
	w := csv.NewWriter(rl)
	rowStrings := make([]string, 0, 1+len(result.GroupBy)+len(result.Fields))
	rowStrings = append(rowStrings, "time")
	for _, dim := range result.GroupBy {
		rowStrings = append(rowStrings, dim)
	}
	for _, field := range result.Fields {
		rowStrings = append(rowStrings, field.Name)
	}
	w.Write(rowStrings)
	w.Flush()

	i := 0
	for {
		row, err := nextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(rl, "Unable to stream response: %v\n", err)
			break
		}
		for j := 0; j < result.NumPeriods; j++ {
			rowStrings := make([]string, 0, 1+len(result.GroupBy)+len(result.Fields))
			rowStrings = append(rowStrings, result.Until.Add(-1*result.Resolution*time.Duration(j)).Format(time.RFC1123))
			for _, dim := range row.Dims {
				rowStrings = append(rowStrings, fmt.Sprint(dim))
			}
			for _, field := range row.Fields {
				rowStrings = append(rowStrings, fmt.Sprint(field[j]))
			}
			w.Write(rowStrings)
			i++
			if i%100 == 0 {
				w.Flush()
			}
		}
	}
	w.Flush()

	return cmds
}
