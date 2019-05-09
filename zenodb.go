package zenodb

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sigar "github.com/cloudfoundry/gosigar"
	"github.com/dustin/go-humanize"
	"github.com/getlantern/goexpr/geo"
	"github.com/getlantern/goexpr/isp"
	geredis "github.com/getlantern/goexpr/redis"
	"github.com/getlantern/golog"
	"github.com/getlantern/vtime"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/metrics"
	"github.com/getlantern/zenodb/planner"
	"github.com/getlantern/zenodb/sql"
	"github.com/oxtoacart/bpool"
	"github.com/rickar/props"
	"github.com/shirou/gopsutil/process"
	"gopkg.in/redis.v5"
)

const (
	defaultMaxBackupWait = 1 * time.Hour

	DefaultIterationCoalesceInterval = 3 * time.Second
	DefaultIterationConcurrency      = 2

	DefaultClusterQueryConcurrency = 25
	DefaultClusterQueryTimeout     = 1 * time.Hour
)

var (
	log = golog.LoggerFor("zenodb")

	systemRAM float64
)

func init() {
	mem := sigar.Mem{}
	err := mem.Get()
	if err != nil {
		panic(fmt.Sprintf("Unable to get system memory: %v", err))
	}
	systemRAM = float64(mem.Total)
}

// DBOpts provides options for configuring the database.
type DBOpts struct {
	// ReadOnly puts the database into a mode whereby it does not persist anything
	// to disk. This is useful for embedding the database in tools like zenomerge.
	ReadOnly bool
	// Dir points at the directory that contains the data files.
	Dir string
	// SchemaFile points at a YAML schema file that configures the tables and
	// views in the database.
	SchemaFile string
	// AliasesFile points at a file that contains expression aliases in the form
	// name=template(%v, %v), with one alias per line.
	AliasesFile string
	// EnableGeo enables geolocation functions
	EnableGeo bool
	// ISPProvider configures a provider of ISP lookups. Specify this to allow the
	// use of ISP functions.
	ISPProvider isp.Provider
	// IPCacheSize determines the size of the ip cache for geo and ISP lookups
	IPCacheSize int
	// RedisClient provides a connection to redis which enables the use of Redis
	// expressions like HGET.
	RedisClient *redis.Client
	// RedisCacheSize controls the size of redis hash caches
	RedisCacheSize int
	// VirtualTime, if true, tells zenodb to use a virtual clock that advances
	// based on the timestamps of Points received via inserts.
	VirtualTime bool
	// WALSyncInterval governs how frequently to sync the WAL to disk. 0 means
	// it syncs after every write (which is not great for performance).
	WALSyncInterval time.Duration
	// MaxWALSize limits how much WAL data to keep (in bytes)
	MaxWALSize int
	// WALCompressionSize specifies the size beyond which to compress WAL segments
	WALCompressionSize int
	// MaxMemoryRatio caps the maximum memory of this process. When the system
	// comes under memory pressure, it will start flushing table memstores.
	MaxMemoryRatio float64
	// IterationCoalesceInterval specifies how long we wait between iteration
	// requests in order to coalesce multiple related ones.
	IterationCoalesceInterval time.Duration
	// IterationConcurrency specifies how many iterations can be performed in
	// parallel
	IterationConcurrency int
	// MaxBackupWait limits how long we're willing to wait for a backup before
	// resuming file operations
	MaxBackupWait time.Duration
	// Passthrough flags this node as a passthrough (won't store data in tables,
	// just WAL). Passthrough nodes will also outsource queries to specific
	// partition handlers. Requires that NumPartitions be specified.
	Passthrough bool
	// NumPartitions identifies how many partitions to split data from
	// passthrough nodes.
	NumPartitions int
	// Partition identies the partition owned by this follower
	Partition int
	// ClusterQueryConcurrency specifies the maximum concurrency for clustered
	// query handlers.
	ClusterQueryConcurrency int
	// ClusterQueryTimeout specifies the maximum amount of time leader will wait
	// for followers to answer a query
	ClusterQueryTimeout time.Duration
	// MaxFollowAge limits how far back to go when follower pulls data from
	// leader
	MaxFollowAge time.Duration
	// Follow is a function that allows a follower to request following a stream
	// from a passthrough node.
	Follow                     func(f func() *common.Follow, cb func(data []byte, newOffset wal.Offset) error)
	RegisterRemoteQueryHandler func(partition int, query planner.QueryClusterFN)
}

type memoryInfo struct {
	mi       *process.MemoryInfoStat
	memstats *runtime.MemStats
}

// DB is a zenodb database.
type DB struct {
	opts                  *DBOpts
	clock                 vtime.Clock
	tables                map[string]*table
	orderedTables         []*table
	walBuffers            *bpool.BytePool
	streams               map[string]*wal.WAL
	newStreamSubscriber   map[string]chan *tableWithOffset
	newStreamSubscriberMx sync.Mutex
	tablesMutex           sync.RWMutex
	isSorting             bool
	nextTableToSort       int
	memory                uint64
	logMemStatsCh         chan *memoryInfo
	flushMutex            sync.Mutex
	followerJoined        chan *follower
	processFollowersOnce  sync.Once
	remoteQueryHandlers   map[int]chan planner.QueryClusterFN
	requestedIterations   chan *iteration
	coalescedIterations   chan []*iteration
	closed                bool
}

// NewDB creates a database using the given options.
func NewDB(opts *DBOpts) (*DB, error) {
	if opts.IterationConcurrency <= 0 {
		opts.IterationConcurrency = DefaultIterationConcurrency
	}

	metrics.SetNumPartitions(opts.NumPartitions)

	var err error
	db := &DB{
		opts:                opts,
		clock:               vtime.RealClock,
		tables:              make(map[string]*table),
		walBuffers:          bpool.NewBytePool(1000, 1024),
		streams:             make(map[string]*wal.WAL),
		newStreamSubscriber: make(map[string]chan *tableWithOffset),
		logMemStatsCh:       make(chan *memoryInfo),
		followerJoined:      make(chan *follower, opts.NumPartitions),
		remoteQueryHandlers: make(map[int]chan planner.QueryClusterFN),
		requestedIterations: make(chan *iteration, 1000), // TODO, make the iteration backlog tunable
		coalescedIterations: make(chan []*iteration, opts.IterationConcurrency),
	}
	if opts.VirtualTime {
		db.clock = vtime.NewVirtualClock(time.Time{})
	}
	if opts.MaxWALSize <= 0 {
		opts.MaxWALSize = 10 * 1024768 // 10 MB
	}
	if opts.WALCompressionSize <= 0 {
		opts.WALCompressionSize = opts.MaxWALSize / 10
	}
	if opts.IterationCoalesceInterval <= 0 {
		opts.IterationCoalesceInterval = DefaultIterationCoalesceInterval
	}
	if opts.MaxBackupWait <= 0 {
		opts.MaxBackupWait = defaultMaxBackupWait
	}
	if opts.ClusterQueryConcurrency <= 0 {
		opts.ClusterQueryConcurrency = DefaultClusterQueryConcurrency
	}
	if opts.ClusterQueryTimeout <= 0 {
		opts.ClusterQueryTimeout = DefaultClusterQueryTimeout
	}

	go db.logMemStats()
	db.opts.ReadOnly = opts.Dir == ""
	if db.opts.ReadOnly {
		log.Debugf("DB is ReadOnly, will not persist data to disk")
	} else {
		// Create db dir
		err = os.MkdirAll(opts.Dir, 0755)
		if err != nil && !os.IsExist(err) {
			return nil, fmt.Errorf("Unable to create db dir at %v: %v", opts.Dir, err)
		}
	}

	if opts.EnableGeo {
		log.Debug("Enabling geolocation functions")
		err = geo.Init(filepath.Join(opts.Dir, "geoip.dat"), opts.IPCacheSize)
		if err != nil {
			return nil, fmt.Errorf("Unable to initialize geo: %v", err)
		}
	}

	if opts.ISPProvider != nil {
		log.Debugf("Setting ISP provider to %v", opts.ISPProvider)
		isp.SetProvider(opts.ISPProvider, opts.IPCacheSize)
	}

	if opts.AliasesFile != "" {
		registerAliases(opts.AliasesFile)
	}

	if opts.RedisClient != nil && opts.RedisCacheSize > 0 {
		log.Debug("Enabling redis expressions")
		geredis.Configure(opts.RedisClient, opts.RedisCacheSize)
	}

	if opts.SchemaFile != "" {
		if db.opts.ReadOnly {
			err = db.ApplySchemaFromFile(opts.SchemaFile)
		} else {
			err = db.pollForSchema(opts.SchemaFile)
		}
		if err != nil {
			return nil, fmt.Errorf("Unable to apply schema: %v", err)
		}
	}
	log.Debugf("Dir: %v    SchemaFile: %v", opts.Dir, opts.SchemaFile)

	if db.opts.RegisterRemoteQueryHandler != nil {
		go db.opts.RegisterRemoteQueryHandler(db.opts.Partition, db.queryForRemote)
	}

	if !db.opts.ReadOnly {
		if db.opts.MaxMemoryRatio > 0 {
			log.Debugf("Limiting maximum memory to %v", humanize.Bytes(db.maxMemoryBytes()))
		}
		go db.trackMemStats()
	}

	if !db.opts.Passthrough {
		go db.coalesceIterations()
		for i := 0; i < db.opts.IterationConcurrency; i++ {
			go db.processIterations()
		}
	}

	return db, err
}

// FlushAll flushes all tables
func (db *DB) FlushAll() {
	db.tablesMutex.Lock()
	for name, table := range db.tables {
		log.Debugf("Force flushing table: %v", name)
		table.forceFlush()
	}
	db.tablesMutex.Unlock()
	log.Debug("Done force flushing tables")
}

func (db *DB) Close() {
	log.Debug("Closing")
	db.tablesMutex.Lock()
	for name, stream := range db.streams {
		log.Debugf("Closing stream %v", name)
		stream.Close()
		delete(db.streams, name)
	}
	db.tablesMutex.Unlock()
	db.FlushAll()
}

func registerAliases(aliasesFile string) {
	log.Debugf("Registering aliases from file at %v", aliasesFile)

	file, err := os.Open(aliasesFile)
	if err != nil {
		log.Errorf("Unable to open aliases file at %v: %v", aliasesFile, err)
		return
	}
	defer file.Close()

	p, err := props.Read(file)
	if err != nil {
		log.Errorf("Unable to read aliases file at %v: %v", aliasesFile, err)
		return
	}

	for _, alias := range p.Names() {
		template := strings.TrimSpace(p.Get(alias))
		alias = strings.TrimSpace(alias)
		sql.RegisterAlias(alias, template)
		log.Debugf("Registered alias %v = %v", alias, template)
	}
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

func (db *DB) now(table string) time.Time {
	return db.clock.Now()
}

func (db *DB) capWALAge(wal *wal.WAL) {
	for {
		time.Sleep(1 * time.Minute)
		db.waitForBackupToFinish()
		err := wal.TruncateToSize(int64(db.opts.MaxWALSize))
		if err != nil {
			log.Errorf("Error truncating WAL: %v", err)
		}
		err = wal.CompressBeforeSize(int64(db.opts.WALCompressionSize))
		if err != nil {
			log.Errorf("Error compressing WAL: %v", err)
		}
	}
}

func (db *DB) trackMemStats() {
	for {
		db.updateMemStats()
		time.Sleep(2 * time.Second)
	}
}

func (db *DB) updateMemStats() {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		log.Errorf("Unable to get process info: %v", err)
		return
	}
	mi, err := p.MemoryInfo()
	if err != nil {
		log.Errorf("Unable to get memory info for process: %v", err)
		return
	}
	memstats := &runtime.MemStats{}
	runtime.ReadMemStats(memstats)
	atomic.StoreUint64(&db.memory, memstats.Alloc)
	mem := &memoryInfo{
		mi:       mi,
		memstats: memstats,
	}
	select {
	case db.logMemStatsCh <- mem:
		// will get logged
	default:
		// won't get logged because we're busy
	}
}

// log the most recent available memstats every 10 seconds
func (db *DB) logMemStats() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	var mem *memoryInfo
	var more bool
	for {
		select {
		case mem, more = <-db.logMemStatsCh:
			if !more {
				return
			}
		case <-t.C:
			if mem != nil {
				mi := mem.mi
				memstats := mem.memstats
				log.Debugf("Memory InUse: %v    Alloc: %v    Sys: %v     RSS: %v", humanize.Bytes(memstats.HeapInuse), humanize.Bytes(memstats.Alloc), humanize.Bytes(memstats.Sys), humanize.Bytes(mi.RSS))
			}
		}
	}
}

// capMemorySize attempts to keep the database's memory size below the
// configured threshold by forcing GC and flushing tables (if allowFlush is
// true). Returns true if it was able to keep the size below the limit, false if
// not.
func (db *DB) capMemorySize(allowFlush bool) bool {
	if db.opts.MaxMemoryRatio <= 0 {
		return true
	}

	actual := atomic.LoadUint64(&db.memory)
	allowed := db.maxMemoryBytes()
	if actual > allowed {
		// First try to regain memory with GC
		log.Debugf("Memory usage of %v exceeds allowed %v, forcing GC", humanize.Bytes(actual), humanize.Bytes(allowed))
		debug.FreeOSMemory()
		db.updateMemStats()
	}

	if !db.opts.Passthrough && allowFlush {
		db.tablesMutex.RLock()
		sizes := make(byCurrentSize, 0, len(db.tables))
		for _, table := range db.tables {
			if !table.Virtual {
				sizes = append(sizes, &memStoreSize{table, table.memStoreSize()})
			}
		}
		db.tablesMutex.RUnlock()

		db.flushMutex.Lock()
		actual = atomic.LoadUint64(&db.memory)
		if actual > allowed {
			// Force flushing on the table with the largest memstore
			sort.Sort(sizes)
			log.Debugf("Memory usage of %v exceeds allowed %v even after GC, forcing flush on %v", humanize.Bytes(actual), humanize.Bytes(allowed), sizes[0].t.Name)
			sizes[0].t.forceFlush()
			db.updateMemStats()
			log.Debugf("Done forcing flush on %v", sizes[0].t.Name)
		}
		db.flushMutex.Unlock()
	}

	actual = atomic.LoadUint64(&db.memory)
	return actual <= allowed
}

func (db *DB) maxMemoryBytes() uint64 {
	return uint64(systemRAM * db.opts.MaxMemoryRatio)
}

type memStoreSize struct {
	t    *table
	size int
}

type byCurrentSize []*memStoreSize

func (a byCurrentSize) Len() int           { return len(a) }
func (a byCurrentSize) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byCurrentSize) Less(i, j int) bool { return a[i].size > a[j].size }

// waitForBackupToFinish waits until there's no .backup_lock file in the dbdir
func (db *DB) waitForBackupToFinish() {
	lockFile := filepath.Join(db.opts.Dir, ".backup_lock")
	start := time.Now()
	for {
		fi, err := os.Stat(lockFile)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			log.Errorf("Unable to stat %v, continuing: %v", lockFile, err)
			return
		}
		if time.Now().Sub(fi.ModTime()) > db.opts.MaxBackupWait {
			log.Debugf("%v is older than %v, continuing", lockFile, db.opts.MaxBackupWait)
			return
		}
		log.Debugf("Waiting for backup to finish")
		time.Sleep(5 * time.Second)
		if time.Now().Sub(start) > db.opts.MaxBackupWait {
			log.Debugf("Waited longer than %v for backup to finish, continuing", db.opts.MaxBackupWait)
			return
		}
	}
}
