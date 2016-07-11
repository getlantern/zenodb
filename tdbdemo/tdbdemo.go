package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/golog"
	"github.com/getlantern/tdb"
)

var (
	log = golog.LoggerFor("tdbdemo")
)

func main() {
	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC)

	tmpDir, err := ioutil.TempDir("", "tdbtest")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	log.Debugf("Writing data to %v", tmpDir)

	numReporters := 5000
	uniquesPerReporter := 100
	uniquesPerPeriod := 20
	reportingPeriods := 1000
	reportingInterval := time.Millisecond
	resolution := reportingInterval * 5
	hotPeriod := resolution * 10
	retainPeriods := 2
	retentionPeriod := time.Duration(retainPeriods) * reportingInterval
	numWriters := 4
	db, err := tdb.NewDB(&tdb.DBOpts{
		Dir:       tmpDir,
		BatchSize: 1000,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = db.CreateTable("test", hotPeriod, retentionPeriod, fmt.Sprintf(`
SELECT
	SUM(i) AS i,
	SUM(ii) AS ii,
	AVG(ii / i) AS iii
FROM inbound
GROUP BY period(%v)`, resolution))
	if err != nil {
		log.Fatal(err)
	}

	inserts := int64(0)
	start := time.Now()

	report := func() {
		stats := db.TableStats("test")
		delta := time.Now().Sub(start)
		start = time.Now()
		i := atomic.SwapInt64(&inserts, 0)
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		preGC := float64(ms.HeapAlloc) / 1024.0 / 1024.0
		runtime.GC()
		runtime.ReadMemStats(&ms)
		postGC := float64(ms.HeapAlloc) / 1024.0 / 1024.0
		fmt.Printf(`
%s inserts at %s inserts per second
Hot Keys: %s     Archived Buckets: %s     Expired Keys: %s
HeapAlloc pre/post GC %f/%f MiB
`,
			humanize.Comma(i), humanize.Comma(i/int64(delta.Seconds())),
			humanize.Comma(stats.HotKeys), humanize.Comma(stats.ArchivedBuckets), humanize.Comma(stats.ExpiredKeys),
			preGC, postGC)
	}

	go func() {
		for {
			tk := time.NewTicker(30 * time.Second)
			for range tk.C {
				report()
			}
		}
	}()

	go func() {
		for {
			tk := time.NewTicker(10 * time.Second)
			for range tk.C {
				now := db.Now("test")
				q, err := db.SQLQuery(fmt.Sprintf(`
SELECT COUNT(i) AS the_count
FROM test ASOF '%v'
GROUP BY period(168h)
`, -2*hotPeriod))
				if err != nil {
					log.Errorf("Unable to build query: %v", err)
					continue
				}
				start := time.Now()
				result, err := q.Run()
				delta := time.Now().Sub(start)
				if err != nil {
					log.Errorf("Unable to run query: %v", err)
					continue
				}
				count := float64(0)
				if len(result.Entries) > 0 {
					count = result.Entries[0].Fields["the_count"][0].Get()
				}
				fmt.Printf("\nQuery at %v returned %v in %v\n", now, humanize.Comma(int64(count)), delta)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numWriters)
	for _w := 0; _w < numWriters; _w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < reportingPeriods; i++ {
				ts := epoch.Add(time.Duration(i) * reportingInterval)
				for r := 0; r < numReporters/numWriters; r++ {
					for u := 0; u < uniquesPerPeriod; u++ {
						p := &tdb.Point{
							Ts: ts,
							Dims: map[string]interface{}{
								"r": rand.Intn(numReporters),
								"u": rand.Intn(uniquesPerReporter),
								"b": rand.Float64() > 0.99,
							},
							Vals: map[string]float64{
								"i":  float64(rand.Intn(100000)),
								"ii": float64(rand.Intn(100)),
							},
						}
						ierr := db.Insert("inbound", p)
						if ierr != nil {
							log.Errorf("Unable to insert: %v", err)
							return
						}
						atomic.AddInt64(&inserts, 1)
					}
				}
				fmt.Print(".")
			}
		}()
	}

	wg.Wait()
	report()
}
