package zenodb

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/golog"
	"github.com/getlantern/vtime"
)

var (
	log = golog.LoggerFor("zenodb")
)

// DBOpts provides options for configuring the database.
type DBOpts struct {
	// SchemaFile points at a YAML schema file that configures the tables and
	// views in the database.
	SchemaFile string
	// Dir points at the directory that contains the data files.
	Dir string
	// DiscardOnBackPressure, when true, tells zenodb to discard new inserts if it
	// is unable to keep up with the insert rate.
	DiscardOnBackPressure bool
	// IncludeMemStoreInQuery, when true, tells zenodb to include the current
	// memstore when performing queries. This requires the memstore to be copied
	// which can dramatically impact performance.
	IncludeMemStoreInQuery bool
	// VirtualTime, if true, tells zenodb to use a virtual clock that advances
	// based on the timestamps of Points received via inserts.
	VirtualTime bool
}

// DB is a zenodb database.
type DB struct {
	opts        *DBOpts
	clock       vtime.Clock
	streams     map[string][]*table
	tables      map[string]*table
	tablesMutex sync.RWMutex
}

// NewDB creates a database using the given options.
func NewDB(opts *DBOpts) (*DB, error) {
	var err error
	db := &DB{opts: opts, clock: vtime.RealClock, tables: make(map[string]*table), streams: make(map[string][]*table)}
	if opts.VirtualTime {
		db.clock = vtime.NewVirtualClock(time.Time{})
	}
	if opts.SchemaFile != "" {
		err = db.pollForSchema(opts.SchemaFile)
	}
	log.Debugf("Dir: %v    SchemaFile: %v", opts.Dir, opts.SchemaFile)
	return db, err
}

// TableStats returns the TableStats for the named table.
func (db *DB) TableStats(table string) TableStats {
	t := db.getTable(table)
	if t == nil {
		return TableStats{}
	}
	t.statsMutex.RLock()
	defer t.statsMutex.RUnlock()
	return t.stats
}

// AllTableStats returns all TableStats for all tables, keyed to the table
// names.
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

// PrintTableStats prints the stats for the named table to a string.
func (db *DB) PrintTableStats(table string) string {
	stats := db.TableStats(table)
	now := db.clock.Now()
	return fmt.Sprintf("%v (%v)\tFiltered: %v    Queued: %v    Inserted: %v    Dropped: %v    Expired: %v",
		table,
		now.In(time.UTC),
		humanize.Comma(stats.FilteredPoints),
		humanize.Comma(stats.QueuedPoints),
		humanize.Comma(stats.InsertedPoints),
		humanize.Comma(stats.DroppedPoints),
		humanize.Comma(stats.ExpiredValues))
}

func (db *DB) getTable(table string) *table {
	db.tablesMutex.RLock()
	t := db.tables[strings.ToLower(table)]
	db.tablesMutex.RUnlock()
	return t
}
