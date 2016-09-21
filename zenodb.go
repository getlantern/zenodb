package zenodb

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/goexpr/geo"
	"github.com/getlantern/goexpr/isp"
	"github.com/getlantern/golog"
	"github.com/getlantern/vtime"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/sql"
)

var (
	log = golog.LoggerFor("zenodb")
)

// DBOpts provides options for configuring the database.
type DBOpts struct {
	// Dir points at the directory that contains the data files.
	Dir string
	// SchemaFile points at a YAML schema file that configures the tables and
	// views in the database.
	SchemaFile string
	// ISPDatabase points at an ISP database like the one from here:
	// https://lite.ip2location.com/database/ip-asn. Specify this to allow the use
	// of ISP functions.
	ISPDatabase string
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
	opts            *DBOpts
	clock           vtime.Clock
	streams         map[string]*wal.WAL
	tables          map[string]*table
	orderedTables   []*table
	tablesMutex     sync.RWMutex
	isSorting       bool
	nextTableToSort int
}

// NewDB creates a database using the given options.
func NewDB(opts *DBOpts) (*DB, error) {
	var err error
	db := &DB{opts: opts, clock: vtime.RealClock, tables: make(map[string]*table), streams: make(map[string]*wal.WAL)}
	if opts.VirtualTime {
		db.clock = vtime.NewVirtualClock(time.Time{})
	}
	if opts.SchemaFile != "" {
		err = db.pollForSchema(opts.SchemaFile)
		if err != nil {
			return nil, fmt.Errorf("Unable to apply schema: %v", err)
		}
	}
	log.Debug("Enabling geolocation functions")
	err = geo.Init(filepath.Join(opts.Dir, "geoip.dat.gz"))
	if err != nil {
		return nil, fmt.Errorf("Unable to initialize geo: %v", err)
	}
	if opts.ISPDatabase != "" {
		log.Debugf("Enabling ISP functions using file at %v", opts.ISPDatabase)
		err = isp.Init(opts.ISPDatabase)
		if err != nil {
			return nil, fmt.Errorf("Unable to initialize ISP functions from file at %v: %v", opts.ISPDatabase, err)
		}
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

func (db *DB) getFields(table string) ([]sql.Field, error) {
	t := db.getTable(table)
	if t == nil {
		return nil, fmt.Errorf("Table '%v' not found", table)
	}
	return t.Fields, nil
}

func (db *DB) getFieldsOptional(table string) ([]sql.Field, error) {
	t := db.getTable(table)
	if t == nil {
		return nil, nil
	}
	return t.Fields, nil
}
