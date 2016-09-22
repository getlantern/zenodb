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
	"github.com/getlantern/zenodb"
	"github.com/jmcvetta/randutil"
)

var (
	log = golog.LoggerFor("zenodbdemo")
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
	targetPointsPerSecond := 40000
	numWriters := 4
	targetPointsPerSecondPerWriter := targetPointsPerSecond / numWriters
	targetDeltaFor1000Points := 1000 * time.Second / time.Duration(targetPointsPerSecondPerWriter)
	log.Debugf("Target delta for 1000 points: %v", targetDeltaFor1000Points)

	reporters := make([]string, 0)
	for i := 0; i < numReporters; i++ {
		reporter, _ := randutil.AlphaStringRange(15, 25)
		reporters = append(reporters, reporter)
	}
	uniques := make([]string, 0)
	for i := 0; i < uniquesPerReporter; i++ {
		unique, _ := randutil.AlphaStringRange(150, 250)
		uniques = append(uniques, unique)
	}

	db, err := zenodb.NewDB(&zenodb.DBOpts{
		SchemaFile: "schema.yaml",
		Dir:        "/tmp/zenodbdemo",
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
			tk := time.NewTicker(10 * time.Minute)
			for range tk.C {
				log.Debug("Running query")
				now := time.Now()
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
				if len(result.Rows) != 1 {
					log.Errorf("Unexpected result rows: %d", len(result.Rows))
					continue
				}
				count = result.Rows[0].Values[0]
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
				uqs := make([]int, 0, uniquesPerPeriod)
				for u := 0; u < uniquesPerPeriod; u++ {
					uqs = append(uqs, rand.Intn(uniquesPerReporter))
				}
				for r := 0; r < numReporters/numWriters; r++ {
					for v := 0; v < valuesPerPeriod; v++ {
						ierr := db.Insert("inbound",
							time.Now(),
							map[string]interface{}{
								"r": reporters[rand.Intn(len(reporters))],
								"u": uniques[uqs[rand.Intn(uniquesPerPeriod)]],
								"b": rand.Float64() > 0.99,
								"x": 1,
							},
							map[string]float64{
								"i":  float64(rand.Intn(100000)),
								"ii": 1,
							})
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
