package main

import (
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
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
	go func() {
		log.Error(http.ListenAndServe("localhost:4000", nil))
	}()

	numReporters := 5000
	uniquesPerReporter := 1000
	uniquesPerPeriod := 100
	valuesPerPeriod := 5000
	reportingPeriods := 100000
	resolution := 5 * time.Minute
	retainPeriods := 24
	retentionPeriod := time.Duration(retainPeriods) * resolution
	targetPointsPerSecond := 200000
	numWriters := 4
	targetPointsPerSecondPerWriter := targetPointsPerSecond / numWriters
	targetDeltaFor1000Points := 1000 * time.Second / time.Duration(targetPointsPerSecondPerWriter)
	log.Debugf("Target delta for 1000 points: %v", targetDeltaFor1000Points)

	db, err := tdb.NewDB(&tdb.DBOpts{
		Dir:                  "/tmp/tdbdemo",
		RocksDBStatsInterval: 60 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = db.CreateTable(&tdb.TableOpts{
		Name:             "test",
		RetentionPeriod:  retentionPeriod,
		MaxMemStoreBytes: 250 * 1024 * 1024,
		MaxFlushLatency:  1 * time.Minute,
		SQL: fmt.Sprintf(`
SELECT
	SUM(i) AS i,
	SUM(ii) AS ii,
	AVG(ii) / AVG(i) AS iii
FROM inbound
GROUP BY period(%v)`, resolution),
	})
	if err != nil {
		log.Fatal(err)
	}

	inserts := int64(0)
	start := time.Now()

	report := func() {
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
%v
HeapAlloc pre/post GC %f/%f MiB
`,
			humanize.Comma(i), humanize.Comma(i/int64(delta.Seconds())),
			db.PrintTableStats("test"),
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
			tk := time.NewTicker(1 * time.Minute)
			for range tk.C {
				log.Debug("Running query")
				now := db.Now("test")
				q, err := db.SQLQuery(`
SELECT SUM(ii) AS the_count
FROM test
GROUP BY period(168h)
`)
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
				if len(result.Entries) != 1 {
					log.Fatalf("Unexpected result entries: %d", len(result.Entries))
				}
				count = result.Entries[0].Value("the_count", 0)
				fmt.Printf("\nQuery at %v returned %v in %v\n", now, humanize.Comma(int64(count)), delta)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numWriters)
	for _w := 0; _w < numWriters; _w++ {
		go func() {
			defer wg.Done()
			c := 0
			start := time.Now()
			for i := 0; i < reportingPeriods; i++ {
				ts := time.Now()
				uniques := make([]int, 0, uniquesPerPeriod)
				for u := 0; u < uniquesPerPeriod; u++ {
					uniques = append(uniques, rand.Intn(uniquesPerReporter))
				}
				for r := 0; r < numReporters/numWriters; r++ {
					for v := 0; v < valuesPerPeriod; v++ {
						p := &tdb.Point{
							Ts: ts,
							Dims: map[string]interface{}{
								"r": rand.Intn(numReporters),
								"u": uniques[rand.Intn(uniquesPerPeriod)],
								"b": rand.Float64() > 0.99,
								"x": 1,
							},
							Vals: map[string]float64{
								"i":  float64(rand.Intn(100000)),
								"ii": 1,
							},
						}
						ierr := db.Insert("inbound", p)
						if ierr != nil {
							log.Errorf("Unable to insert: %v", err)
							return
						}
						atomic.AddInt64(&inserts, 1)
						c++

						// Control rate
						if c > 0 && c%1000 == 0 {
							delta := time.Now().Sub(start)
							if delta < targetDeltaFor1000Points {
								time.Sleep(targetDeltaFor1000Points - delta)
							}
							start = time.Now()
						}
					}
				}
				fmt.Print(".")
			}
		}()
	}

	wg.Wait()
	report()
}
