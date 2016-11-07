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
	// ISPProvider configures a provider of ISP lookups. Specify this to allow the
	// use of ISP functions.
	ISPProvider isp.Provider
	// IncludeMemStoreInQuery, when true, tells zenodb to include the current
	// memstore when performing queries. This requires the memstore to be copied
	// which can dramatically impact performance.
	IncludeMemStoreInQuery bool
	// VirtualTime, if true, tells zenodb to use a virtual clock that advances
	// based on the timestamps of Points received via inserts.
	VirtualTime bool
	// WALSyncInterval governs how frequently to sync the WAL to disk. 0 means
	// it syncs after every write (which is not great for performance).
	WALSyncInterval time.Duration
	// MaxWALAge limits how far back we keep WAL files.
	MaxWALAge time.Duration
	// WALCompressionAge sets a cutoff for the age of WAL files that will be
	// gzipped
	WALCompressionAge time.Duration
	// Leader flags this node as a leader (won't store data in tables, just WAL).
	Leader bool
	// NumPartitions identifies how many partitions to split
	NumPartitions int
	// Partition identies the partition owned by this follower
	Partition int
	// Follow is a function that allows a follower to request following a stream
	// from its leader.
	Follow                     func(f *Follow, cb func(data []byte, newOffset wal.Offset) error)
	RegisterRemoteQueryHandler func(r *RegisterQueryHandler, query func(sql string, onEntry func(*Entry) error) error)
}

// DB is a zenodb database.
type DB struct {
	opts                *DBOpts
	clock               vtime.Clock
	streams             map[string]*wal.WAL
	tables              map[string]*table
	orderedTables       []*table
	tablesMutex         sync.RWMutex
	isSorting           bool
	nextTableToSort     int
	remoteQueryHandlers map[int][]QueryRemote
}

// NewDB creates a database using the given options.
func NewDB(opts *DBOpts) (*DB, error) {
	var err error
	db := &DB{
		opts:                opts,
		clock:               vtime.RealClock,
		tables:              make(map[string]*table),
		streams:             make(map[string]*wal.WAL),
		remoteQueryHandlers: make(map[int][]QueryRemote),
	}
	if opts.VirtualTime {
		db.clock = vtime.NewVirtualClock(time.Time{})
	}
	if opts.MaxWALAge == 0 {
		opts.MaxWALAge = 24 * time.Hour
	}
	if opts.WALCompressionAge == 0 {
		opts.WALCompressionAge = opts.MaxWALAge / 10
	}
	log.Debug("Enabling geolocation functions")
	err = geo.Init(filepath.Join(opts.Dir, "geoip.dat.gz"))
	if err != nil {
		return nil, fmt.Errorf("Unable to initialize geo: %v", err)
	}
	if opts.ISPProvider != nil {
		isp.SetProvider(opts.ISPProvider)
	}
	if opts.SchemaFile != "" {
		err = db.pollForSchema(opts.SchemaFile)
		if err != nil {
			return nil, fmt.Errorf("Unable to apply schema: %v", err)
		}
	}
	log.Debugf("Dir: %v    SchemaFile: %v", opts.Dir, opts.SchemaFile)
	if db.opts.RegisterRemoteQueryHandler != nil {
		go db.opts.RegisterRemoteQueryHandler(&RegisterQueryHandler{db.opts.Partition}, db.QueryForRemote)
	}
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

func (db *DB) getTableForQuery(table string, sql string) (t queryable) {
	tbl := db.getTable(table)
	if tbl == nil {
		return nil
	}
	t = tbl
	if db.opts.Leader {
		t = &remoteQueryable{tbl, db, sql}
	}
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

func (db *DB) capWALAge(wal *wal.WAL) {
	for {
		time.Sleep(1 * time.Minute)
		err := wal.TruncateBeforeTime(time.Now().Add(-1 * db.opts.MaxWALAge))
		if err != nil {
			log.Errorf("Error truncating WAL: %v", err)
		}
		err = wal.CompressBeforeTime(time.Now().Add(-1 * db.opts.WALCompressionAge))
		if err != nil {
			log.Errorf("Error compressing WAL: %v", err)
		}
	}
}
