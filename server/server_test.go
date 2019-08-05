package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/getlantern/errors"
	"github.com/getlantern/golog"
	"github.com/getlantern/grtrack"
	. "github.com/getlantern/waitforserver"
	"github.com/getlantern/withtimeout"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/rpc"
	"github.com/getlantern/zenodb/testsupport"

	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	clusterQueryConcurrency = 100
)

var (
	log = golog.LoggerFor("server_test")
)

func TestRoundTimeUp(t *testing.T) {
	ts := time.Date(2015, 5, 6, 7, 8, 9, 10, time.UTC)
	rounded := encoding.RoundTimeUp(ts, time.Second)
	expected := time.Date(2015, 5, 6, 7, 8, 10, 0, time.UTC)
	assert.Equal(t, expected, rounded)
}

func TestServers(t *testing.T) {
	cancel := testsupport.RedirectLogsToTest(t)
	defer cancel()

	type testTask struct {
		name string
		fn   func(t *testing.T)
	}

	iters := 8
	concurrency := runtime.NumCPU() * 2

	testTasks := make(chan testTask, iters*concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()

			for task := range testTasks {
				if !t.Run(task.name, task.fn) {
					t.Fatal("done")
					break
				}
			}
		}()
	}
	defer wg.Wait()

	enqueueTest := func(name string, fn func(t *testing.T)) {
		testTasks <- testTask{name, fn}
	}

	rpcPort := func(base, i int) int {
		return (base + i) * 100
	}
	httpPort := func(base, i int) int {
		return (base + ((i + 1) * iters) + i) * 100
	}
	for _i := 1; _i <= iters; _i++ {
		i := _i
		enqueueTest(fmt.Sprintf("SingleDB.%d", i), func(t *testing.T) {
			doTest(t, nil, func(tmpDir func(string) string, tmpFile string) ([]*Server, [][]*Server) {
				s := &Server{
					DBDir:                     tmpDir(fmt.Sprintf("singledb.%d", i)),
					Schema:                    tmpFile,
					Addr:                      fmt.Sprintf("127.0.0.1:%d", rpcPort(100, i)),
					HTTPSAddr:                 fmt.Sprintf("127.0.0.1:%d", httpPort(100, i)),
					Insecure:                  true,
					IterationCoalesceInterval: 1 * time.Millisecond,
					Panic:                     dontPanic,
				}
				return []*Server{s}, nil
			})
		})

		enqueueTest(fmt.Sprintf("ClusterSimple.%d", i), func(t *testing.T) {
			doTestCluster(t, 1, 3, 1, i, rpcPort(200, i), httpPort(200, i))
		})
	}

	close(testTasks)

	// t.Run("SingleDB", testSingleDB)
	// t.Run("ClusterSimple", testClusterSimple)
	// t.Run("ClusterRedundantFollowers", func(t *testing.T) { doTestCluster(t, 1, 1, 2, 52000); wg.Done() })
	// t.Run("ClusterMultiLeader", testClusterMultiLeader)
	// t.Run("ClusterComplex", testClusterComplex)
}

// func testSingleDB(t *testing.T) {
// 	doTest(t, nil, func(tmpDir func() string, tmpFile string) ([]*Server, [][]*Server) {
// 		s := &Server{
// 			DBDir:                     tmpDir(),
// 			Schema:                    tmpFile,
// 			Addr:                      "127.0.0.1:40000",
// 			HTTPSAddr:                 "127.0.0.1:41000",
// 			Insecure:                  true,
// 			IterationCoalesceInterval: 1 * time.Millisecond,
// 		}
// 		return []*Server{s}, nil
// 	})
// }

// func testClusterSimple(t *testing.T) {
// 	doTestCluster(t, 1, 3, 1, 50000)
// }

// func testClusterRedundantFollowers(t *testing.T) {
// 	doTestCluster(t, 1, 1, 2, 52000)
// }

// func testClusterMultiLeader(t *testing.T) {
// 	doTestCluster(t, 2, 2, 1, 54000)
// }

// func testClusterComplex(t *testing.T) {
// 	doTestCluster(t, 2, 3, 3, 56000)
// }

func doTestCluster(t *testing.T, numLeaders int, numPartitions int, redundancyLevel int, iteration int, startingPortRPC, startingPortHTTP int) {
	t.Helper()

	doTest(t, nil, func(tmpDir func(string) string, tmpFile string) ([]*Server, [][]*Server) {
		var leaderAddrs []string
		var leaders []*Server
		for i := 0; i < numLeaders; i++ {
			id := iteration*10 + (i+1)*9
			leader := &Server{
				DBDir:         tmpDir(fmt.Sprintf("cluster.leader.%d.%d.%d.%d", numLeaders, numPartitions, redundancyLevel, id)),
				Schema:        tmpFile,
				Addr:          fmt.Sprintf("127.0.0.1:%d", startingPortRPC+i),
				HTTPSAddr:     fmt.Sprintf("127.0.0.1:%d", startingPortHTTP+i),
				Insecure:      true,
				ID:            id,
				NumPartitions: numPartitions,
				Passthrough:   true,
				AllowZeroID:   i == 0,
				ListenTimeout: 10 * time.Second,
				Panic:         dontPanic,
			}
			leaderAddrs = append(leaderAddrs, fmt.Sprintf("%v%v%v", leader.Addr, sourceIDDelim, id)) // use an ID that doesn't start at 0 to make sure we're not treating indexes like magic leader ids
			leaders = append(leaders, leader)
		}
		leaderAddrsString := strings.Join(leaderAddrs, ",")
		t.Logf("Leader addrs: %v", leaderAddrsString)

		var followers [][]*Server
		for i := 0; i < numPartitions; i++ {
			var partitionFollowers []*Server
			for j := 0; j < redundancyLevel; j++ {
				id := iteration*10 + j
				follower := &Server{
					DBDir:                     tmpDir(fmt.Sprintf("cluster.follower.%d.%d.%d.%d", numLeaders, numPartitions, redundancyLevel, id)),
					Schema:                    tmpFile,
					Addr:                      fmt.Sprintf("127.0.0.1:%d", startingPortRPC+(i+1)*10+(j+1)*100),
					HTTPSAddr:                 fmt.Sprintf("127.0.0.1:%d", startingPortHTTP+(i+1)*10+(j+1)*100),
					Insecure:                  true,
					ID:                        id,
					AllowZeroID:               j == 0,
					NumPartitions:             numPartitions,
					Partition:                 i,
					Capture:                   leaderAddrsString,
					Feed:                      leaderAddrsString,
					IterationCoalesceInterval: 1 * time.Millisecond,
					ListenTimeout:             10 * time.Second,
					MaxReconnectWaitTime:      250 * time.Millisecond,
					Panic:                     dontPanic,
				}
				partitionFollowers = append(partitionFollowers, follower)
			}
			followers = append(followers, partitionFollowers)
		}
		return leaders, followers
	})
}

func doTest(t *testing.T, partitionKeys []string, configureCluster func(tmpDirs func(string) string, tmpFile string) ([]*Server, [][]*Server)) {
	gr := grtrack.Start()

	sleepBeforeStart := false
	startServer := func(s *Server) error {
		if sleepBeforeStart {
			// sometimes sleep to test followers joining simultaneously and at separate times
			time.Sleep(5 * time.Second)
		}
		sleepBeforeStart = !sleepBeforeStart
		_, run, err := s.Prepare()
		if err == nil {
			go run()
		}
		return err
	}

	closeServer := func(s *Server) error {
		// closes the server and fails if closing takes more than 15 seconds
		_, timedOut, _ := withtimeout.Do(15*time.Second, func() (interface{}, error) {
			s.Close()
			return nil, nil
		})
		if timedOut {
			gr.Check(t)
			return errors.New("Failed to close within 15 seconds")
		}
		return nil
	}

	flushLatency := 250 * time.Millisecond
	tmpFile, err := ioutil.TempFile("", "zenodbschema")
	if !assert.NoError(t, err, "Unable to create temp file") {
		return
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	partitionClause := ""
	if len(partitionKeys) > 0 {
		partitionClause = fmt.Sprintf("\n  partitionby: [%v]\n", strings.Join(partitionKeys, ","))
	}
	schema := fmt.Sprintf(`
test:
  maxflushlatency: %s
  retentionperiod: 5h%s
  sql: >
    SELECT
      val,
      IF(meta = 'include', SUM(val)) AS filtered_val
    FROM inbound
    GROUP BY a, b, period(1h)
test_a:
  maxflushlatency: %s
  retentionperiod: 5h%s
	partitionby: [a]
  sql: >
    SELECT
      val,
      IF(meta = 'include', SUM(val)) AS filtered_val
    FROM inbound
    GROUP BY a, b, period(1h)
test_b:
  maxflushlatency: %s
  retentionperiod: 5h%s
	partitionby: [b]
  sql: >
    SELECT
      val,
      IF(meta = 'include', SUM(val)) AS filtered_val
    FROM inbound
    GROUP BY a, b, period(1h)
test_ab:
  maxflushlatency: %s
  retentionperiod: 5h%s
	partitionby: [a, b]
  sql: >
    SELECT
      val,
      IF(meta = 'include', SUM(val)) AS filtered_val
    FROM inbound
    GROUP BY a, b, period(1h)
`, flushLatency, partitionClause, flushLatency, partitionClause, flushLatency, partitionClause, flushLatency, partitionClause)
	schema = strings.Replace(schema, "\t", "  ", -1)
	err = ioutil.WriteFile(tmpFile.Name(), []byte(schema), 0644)
	if !assert.NoError(t, err, "Unable to write schema") {
		return
	}

	var tmpDirs []string
	buildCluster := func() ([]*Server, [][]*Server) {
		t.Helper()
		tmpDir := func(prefix string) string {
			tmpDir, err := ioutil.TempDir("", fmt.Sprintf("zenodbtest.%v", prefix))
			if err != nil {
				t.Fatalf("Unable to create tmpDir: %v", err)
			}
			tmpDirs = append(tmpDirs, tmpDir)
			return tmpDir
		}

		return configureCluster(tmpDir, tmpFile.Name())
	}

	rebuildCluster := func() ([]*Server, [][]*Server) {
		t.Helper()
		// reuse existing directories
		i := 0
		tmpDir := func(prefix string) string {
			dir := tmpDirs[i]
			i++
			return dir
		}

		return configureCluster(tmpDir, tmpFile.Name())
	}

	defer func() {
		for _, tmpDir := range tmpDirs {
			if t.Failed() {
				t.Logf("Temporary files kept at %v", tmpDir)
			} else {
				os.RemoveAll(tmpDir)
			}
		}
	}()

	log.Debug("Building initial cluster")
	leaders, followersByPartition := buildCluster()
	for _, leader := range leaders {
		if err := startServer(leader); !assert.NoError(t, err, "Unable to start leader") {
			return
		}
	}
	for _, followersForPartition := range followersByPartition {
		for _, follower := range followersForPartition {
			if err := startServer(follower); !assert.NoError(t, err, "Unable to start follower") {
				return
			}
		}
	}

	clients, closeClients, err := clientsForServers(leaders)
	if !assert.NoError(t, err) {
		return
	}
	defer closeClients()

	now := time.Now()
	inserted := 0
	insert := func(iters int) bool {
		t.Helper()
		log.Debugf("Inserting %d", iters)
		var inserters []rpc.Inserter
		for _, client := range clients {
			inserter, err := client.NewInserter(context.Background(), "inbound")
			if !assert.NoError(t, err) {
				return false
			}
			inserters = append(inserters, inserter)
		}

		for i := 0; i < iters; i++ {
			inserter := inserters[i%len(inserters)]
			meta := ""
			if i%2 == 0 {
				meta = "include"
			}
			insertErr := inserter.Insert(
				now,
				map[string]interface{}{
					"a":    i,
					"b":    "thing",
					"meta": meta,
				},
				func(cb func(key string, value float64)) {
					cb("val", 1)
				},
			)
			if !assert.NoError(t, insertErr) {
				return false
			}
			inserted++
		}

		return true
	}

	query := func(client rpc.Client, sql string, includeMemStore bool) (*common.QueryMetaData, []*core.FlatRow, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		md, iterate, err := client.Query(ctx, sql, includeMemStore)
		if err != nil {
			return nil, nil, err
		}
		var result []*core.FlatRow
		_, err = iterate(func(row *core.FlatRow) (bool, error) {
			result = append(result, row)
			return true, nil
		})
		return md, result, err
	}

	sql := "SELECT * FROM test GROUP BY b"

	type test struct {
		label     string
		retryUpTo time.Duration
		prep      func() bool
	}

	runTest := func(tst test) bool {
		t.Helper()
		if tst.prep != nil {
			log.Debugf("Preparing to check that %v", tst.label)
			if !tst.prep() {
				return false
			}
		}

		log.Debugf("Checking that %v", tst.label)

		expectedFields := []string{"_points", "val", "filtered_val"}
		er := testsupport.ExpectedResult{
			testsupport.ExpectedRow{
				now.Truncate(time.Hour).Add(time.Hour),
				map[string]interface{}{
					"b": "thing",
				},
				map[string]float64{
					"_points":      float64(inserted),
					"val":          float64(inserted),
					"filtered_val": math.Floor(float64(inserted) / 2),
				},
			},
		}

		printStats := func() {
			for _, leader := range leaders {
				leader.log.Debug(leader.db.PrintTableStats("test"))
			}
			for _, followers := range followersByPartition {
				for _, follower := range followers {
					follower.log.Debug(follower.db.PrintTableStats("test"))
				}
			}
		}

		// Keep querying until we succeed or run out of time
		first := true
	retryLoop:
		for start := time.Now(); time.Now().Sub(start) < tst.retryUpTo; {
			if !first {
				time.Sleep(tst.retryUpTo / 20)
			}
			first = false
			printStats()
			for _, client := range clients {
				// query multiple times to make sure we're hitting all the different followers
				numQueries := 1
				if len(followersByPartition) > 0 {
					numQueries = 0
					for _, followers := range followersByPartition {
						numQueries += len(followers)
					}
				}
				for i := 0; i < numQueries; i++ {
					md, rows, err := query(client, sql, true)
					if err != nil {
						continue retryLoop
					}
					if !reflect.DeepEqual(expectedFields, md.FieldNames) {
						continue retryLoop
					}
					if !er.TryAssert(md, rows) {
						continue retryLoop
					}
				}
			}
			log.Debug("queries succeeded, moving on to final assertions")
			break retryLoop
		}

		// Now assert everything to get proper test results
		for _, client := range clients {
			// query multiple times to make sure we're hitting all the different followers
			numQueries := 1
			if len(followersByPartition) > 0 {
				numQueries = 0
				for _, followers := range followersByPartition {
					numQueries += len(followers)
				}
			}
			for i := 0; i < numQueries; i++ {
				md, rows, err := query(client, sql, true)
				if !assert.NoError(t, err, "%v, unable to query client", tst.label) {
					return false
				}
				if !assert.EqualValues(t, expectedFields, md.FieldNames, "%v, wrong field names in query result", tst.label) {
					return false
				}
				if !er.Assert(t, fmt.Sprintf("%v (%d)", tst.label, i), md, rows) {
					for partition, followersForPartition := range followersByPartition {
						followerClients, closeClients, err := clientsForServers(followersForPartition)
						if !assert.NoError(t, err, "Unable to get clients") {
							return false
						}
						defer closeClients()
						for followerIdx, followerClient := range followerClients {
							_, rows, err := query(followerClient, sql, true)
							if err != nil {
								t.Logf("Partition: %d   Follower Idx: %d   Error: %v", partition, followerIdx, err)
							} else {
								for _, row := range rows {
									t.Logf("Partition: %d   Follower Idx: %d   Row: %v %v", partition, followerIdx, row.Key.AsMap(), row.Values)
								}
							}
						}
					}
					return false
				}
			}
		}

		return true
	}

	runTests := func(tests []test) {
		t.Helper()
		for _, tst := range tests {
			if ok := runTest(tst); !ok {
				log.Debugf("Failed on %v failed, stopping", tst.label)
				return
			}
		}
	}

	runTests([]test{
		test{"inserted data should become available", 10 * time.Second, func() bool {
			if !insert(10000) {
				return false
			}
			return true
		}},
		test{"remaining followers return correct results after killing redundant followers", 10 * time.Second, func() bool {
			for _, followersForPartition := range followersByPartition {
				if len(followersForPartition) > 1 {
					for i := 1; i < len(followersForPartition); i++ {
						if err := closeServer(followersForPartition[i]); !assert.NoError(t, err, "Unable to close follower") {
							return false
						}
					}
				}
			}

			return true
		}},
		test{"followers reconnect after leaders are restarted", 10 * time.Second, func() bool {
			if !insert(10000) {
				return false
			}

			// Sleep a little bit to make sure the inserts are captured
			// TODO: maybe we should make wal capture synchronous again
			time.Sleep(2 * time.Second)

			for _, leader := range leaders {
				if err := closeServer(leader); !assert.NoError(t, err, "Unable to close leader") {
					return false
				}
			}
			leaders, _ = rebuildCluster()
			for _, leader := range leaders {
				if err := startServer(leader); !assert.NoError(t, err, "Unable to start leader") {
					return false
				}
			}
			clients, closeClients, err = clientsForServers(leaders)
			if !assert.NoError(t, err) {
				return false
			}

			return true
		}},
		test{"followers reconnect to leaders after restarting", 10 * time.Second, func() bool {
			for _, followersForPartition := range followersByPartition {
				for _, follower := range followersForPartition {
					if err := closeServer(follower); !assert.NoError(t, err, "Unable to close follower") {
						return false
					}
				}
			}
			_, followersByPartition = rebuildCluster()
			for _, followersForPartition := range followersByPartition {
				for _, follower := range followersForPartition {
					if err := startServer(follower); !assert.NoError(t, err, "Unable to start follower") {
						return false
					}
				}
			}

			return true
		}},
		test{"redundant followers read the data they missed while offline", 10 * time.Second, func() bool {
			for _, followersForPartition := range followersByPartition {
				if len(followersForPartition) > 1 {
					if err := closeServer(followersForPartition[0]); !assert.NoError(t, err) {
						return false
					}
				}
			}

			if !insert(1) {
				return false
			}

			log.Debug("restart followers")
			for _, followersForPartition := range followersByPartition {
				for _, follower := range followersForPartition {
					closeServer(follower)
				}
			}
			_, followersByPartition = rebuildCluster()
			for _, followersForPartition := range followersByPartition {
				for _, follower := range followersForPartition {
					startServer(follower)
				}
			}

			return true
		}},
	})

	if closeClients != nil {
		closeClients()
	}

	log.Debug("Make sure we can shut down whole cluster")
	for _, followersForPartition := range followersByPartition {
		for _, follower := range followersForPartition {
			closeServer(follower)
		}
	}
	for _, leader := range leaders {
		closeServer(leader)
	}
}

func clientsForServers(servers []*Server) ([]rpc.Client, func(), error) {
	var clients []rpc.Client
	for _, s := range servers {
		if err := WaitForServer("tcp", s.Addr, 5*time.Second); err != nil {
			return nil, nil, err
		}
		client, err := rpc.Dial(s.Addr, &rpc.ClientOpts{
			Dialer: func(addr string, timeout time.Duration) (net.Conn, error) {
				return tls.DialWithDialer(
					&net.Dialer{Timeout: timeout},
					"tcp",
					addr,
					&tls.Config{InsecureSkipVerify: true})
			},
		})
		if err != nil {
			return nil, nil, err
		}
		clients = append(clients, client)
	}
	return clients,
		func() {
			for _, client := range clients {
				client.Close()
			}
		},
		nil
}

func dontPanic(err interface{}) {
	log.Debugf("I'm not panicking about: %v", err)
}
