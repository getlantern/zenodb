package tdb

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/golog"
)

var (
	log = golog.LoggerFor("tdb")
)

type DBOpts struct {
	SchemaFile            string
	Dir                   string
	DiscardOnBackPressure bool
	RocksDBStatsInterval  time.Duration
}

type DB struct {
	opts        *DBOpts
	streams     map[string][]*table
	tables      map[string]*table
	tablesMutex sync.RWMutex
}

func NewDB(opts *DBOpts) (*DB, error) {
	var err error
	db := &DB{opts: opts, tables: make(map[string]*table), streams: make(map[string][]*table)}
	if opts.SchemaFile != "" {
		err = db.pollForSchema(opts.SchemaFile)
	}
	log.Debugf("Dir: %v    SchemaFile: %v", opts.Dir, opts.SchemaFile)
	return db, err
}

func (db *DB) TableStats(table string) TableStats {
	t := db.getTable(table)
	if t == nil {
		return TableStats{}
	}
	t.statsMutex.RLock()
	defer t.statsMutex.RUnlock()
	return t.stats
}

func (db *DB) AllTableStats() map[string]TableStats {
	m := make(map[string]TableStats)
	tables := make(map[string]*table, 0)
	db.tablesMutex.RLock()
	for name, t := range db.tables {
		tables[name] = t
	}
	db.tablesMutex.RUnlock()
	for name, t := range tables {
		t.statsMutex.RLock()
		m[name] = t.stats
		t.statsMutex.RUnlock()
	}
	return m
}

func (db *DB) PrintTableStats(table string) string {
	stats := db.TableStats(table)
	now := db.Now(table)
	return fmt.Sprintf("%v (%v)\tFiltered: %v    Queued: %v    Inserted: %v    Dropped: %v    Expired: %v",
		table,
		now.In(time.UTC),
		humanize.Comma(stats.FilteredPoints),
		humanize.Comma(stats.QueuedPoints),
		humanize.Comma(stats.InsertedPoints),
		humanize.Comma(stats.DroppedPoints),
		humanize.Comma(stats.ExpiredValues))
}

func (db *DB) Now(table string) time.Time {
	t := db.getTable(table)
	if t == nil {
		return time.Time{}
	}
	return t.clock.Now()
}

func (db *DB) getTable(table string) *table {
	db.tablesMutex.RLock()
	t := db.tables[strings.ToLower(table)]
	db.tablesMutex.RUnlock()
	return t
}
