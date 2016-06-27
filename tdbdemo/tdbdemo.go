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
	"github.com/oxtoacart/tdb"
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
	uniquesPerReporter := 500
	uniquesPerPeriod := 20
	reportingPeriods := 1000
	reportingInterval := time.Millisecond
	resolution := reportingInterval * 5
	hotPeriod := resolution * 10
	retentionPeriod := time.Duration(reportingPeriods) * reportingInterval * 2
	numWriters := 4
	db := tdb.NewDB(tmpDir)
	err = db.CreateTable("test", resolution, hotPeriod, retentionPeriod)
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
Hot Keys: %s     Hot Buckets: %s     Archived Buckets: %s
HeapAlloc pre/post GC %f/%f MiB
`,
			humanize.Comma(i), humanize.Comma(i/int64(delta.Seconds())),
			humanize.Comma(stats.HotKeys), humanize.Comma(stats.HotBuckets), humanize.Comma(stats.ArchivedBuckets),
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
						ierr := db.Insert("test", p)
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
