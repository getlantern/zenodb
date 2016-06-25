package tdb

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "tdbtest")
	if !assert.NoError(t, err, "Unable to create temporary directory") {
		return
	}
	defer os.RemoveAll(tmpDir)

	numReporters := 5000
	uniquesPerReporter := 100
	uniquesPerPeriod := 20
	reportingPeriods := 100
	reportingInterval := time.Millisecond
	resolution := reportingInterval * 10
	hotPeriod := resolution * 3
	retentionPeriod := time.Duration(reportingPeriods) * reportingInterval * 2
	numWriters := 4
	db := NewDB(tmpDir)
	err = db.CreateTable("test", resolution, hotPeriod, retentionPeriod)
	if !assert.NoError(t, err, "Unable to create database") {
		return
	}
	start := time.Now()
	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	wg.Add(numWriters)
	for _w := 0; _w < numWriters; _w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < reportingPeriods; i++ {
				ts := epoch.Add(time.Duration(i) * reportingInterval)
				for r := 0; r < numReporters/numWriters; r++ {
					for u := 0; u < uniquesPerPeriod; u++ {
						p := &Point{
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
						if !assert.NoError(t, ierr, "Unable to insert") {
							return
						}
					}
				}
				fmt.Print(".")
			}
		}()
	}

	wg.Wait()
	delta := time.Now().Sub(start)
	inserts := reportingPeriods * numReporters * uniquesPerPeriod
	fmt.Printf("\n%d inserts at %d inserts per second\n", inserts, inserts/int(delta.Seconds()))
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	preGC := float64(ms.HeapAlloc) / 1024.0 / 1024.0
	runtime.GC()
	runtime.ReadMemStats(&ms)
	postGC := float64(ms.HeapAlloc) / 1024.0 / 1024.0
	fmt.Printf("HeapAlloc pre/post GC %f/%f MiB\n", preGC, postGC)

	numBuckets := 0
	for _, s := range db.tables["test"].series {
		b := s.tail
		for {
			if b == nil {
				break
			}
			numBuckets++
			b = b.prev
		}
	}

	fmt.Printf("%d series, %d buckets\n", len(db.tables["test"].series), numBuckets)
}
