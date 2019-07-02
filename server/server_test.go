package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

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

func TestSingleDB(t *testing.T) {
	doTest(t, nil, func(tmpDir func() string, tmpFile string) ([]*Server, [][]*Server) {
		s := &Server{
			DBDir:                     tmpDir(),
			Schema:                    tmpFile,
			Addr:                      "127.0.0.1:40000",
			HTTPSAddr:                 "127.0.0.1:41000",
			Insecure:                  true,
			IterationCoalesceInterval: 1 * time.Millisecond,
		}
		return []*Server{s}, nil
	})
}

func TestClusterSimple(t *testing.T) {
	doTestCluster(t, 1, 3, 1, 50000)
}

func TestClusterRedundantFollowers(t *testing.T) {
	doTestCluster(t, 1, 1, 2, 52000)
}

func TestClusterMultiLeader(t *testing.T) {
	doTestCluster(t, 2, 2, 1, 54000)
}

func TestClusterComplex(t *testing.T) {
	doTestCluster(t, 2, 3, 3, 56000)
}

func doTestCluster(t *testing.T, numLeaders int, numPartitions int, redundancyLevel int, startingPort int) {
	doTest(t, nil, func(tmpDir func() string, tmpFile string) ([]*Server, [][]*Server) {
		var leaderAddrs []string
		var leaders []*Server
		for i := 0; i < numLeaders; i++ {
			id := (i + 1) * 9
			leader := &Server{
				DBDir:         tmpDir(),
				Schema:        tmpFile,
				Addr:          fmt.Sprintf("127.0.0.1:%d", startingPort+i),
				HTTPSAddr:     fmt.Sprintf("127.0.0.1:%d", startingPort+1000+i),
				Insecure:      true,
				ID:            id,
				NumPartitions: numPartitions,
				Passthrough:   true,
				ListenTimeout: 10 * time.Second,
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
				follower := &Server{
					DBDir:                     tmpDir(),
					Schema:                    tmpFile,
					Addr:                      fmt.Sprintf("127.0.0.1:%d", startingPort+(i+1)*10+(j+1)*100),
					HTTPSAddr:                 fmt.Sprintf("127.0.0.1:%d", startingPort+1000+(i+1)*10+(j+1)*100),
					Insecure:                  true,
					ID:                        j,
					NumPartitions:             numPartitions,
					Partition:                 i,
					Capture:                   leaderAddrsString,
					Feed:                      leaderAddrsString,
					IterationCoalesceInterval: 1 * time.Millisecond,
					ListenTimeout:             10 * time.Second,
				}
				partitionFollowers = append(partitionFollowers, follower)
			}
			followers = append(followers, partitionFollowers)
		}
		return leaders, followers
	})
}

func doTest(t *testing.T, partitionKeys []string, configureCluster func(tmpDirs func() string, tmpFile string) ([]*Server, [][]*Server)) {
	gr := grtrack.Start()

	startServer := func(s *Server) {
		if rand.Float64() > 0.5 {
			// sometimes sleep to test followers joining simultaneously and at separate times
			time.Sleep(1 * time.Second)
		}
		_, err := s.Serve()
		if err != nil {
			log.Fatalf("Unable to start serving: %v", err)
		}
		s.db.Go(func(stop <-chan interface{}) {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					s.log.Debug(s.db.PrintTableStats("test"))
				}
			}
		})
	}

	closeServer := func(s *Server) {
		// closes the server and fails if closing takes more than 15 seconds
		_, timedOut, _ := withtimeout.Do(15*time.Second, func() (interface{}, error) {
			s.Close()
			return nil, nil
		})
		if timedOut {
			gr.Check(t)
			s.log.Error("Failed to close within 15 seconds")
			t.Fatalf("Server failed to close within 15 seconds")
		}
	}

	flushLatency := 10 * time.Second
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
      SUM(val) AS val
    FROM inbound
    GROUP BY period(1h)
`, flushLatency, partitionClause)
	err = ioutil.WriteFile(tmpFile.Name(), []byte(schema), 0644)
	if !assert.NoError(t, err, "Unable to write schema") {
		return
	}

	var tmpDirs []string
	buildCluster := func() ([]*Server, [][]*Server) {
		tmpDir := func() string {
			tmpDir, err := ioutil.TempDir("", "zenodbtest")
			if err != nil {
				t.Fatalf("Unable to create tmpDir: %v", err)
			}
			tmpDirs = append(tmpDirs, tmpDir)
			return tmpDir
		}

		return configureCluster(tmpDir, tmpFile.Name())
	}

	rebuildCluster := func() ([]*Server, [][]*Server) {
		// reuse existing directories
		i := 0
		tmpDir := func() string {
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
		startServer(leader)
	}
	for _, followersForPartition := range followersByPartition {
		for _, follower := range followersForPartition {
			startServer(follower)
		}
	}

	clients, err := clientsForServers(leaders)
	if !assert.NoError(t, err) {
		return
	}

	now := time.Now()
	inserted := 0
	insert := func(iters int) {
		log.Debugf("Inserting %d", iters)
		var inserters []rpc.Inserter
		for _, client := range clients {
			inserter, err := client.NewInserter(context.Background(), "inbound")
			if !assert.NoError(t, err) {
				return
			}
			inserters = append(inserters, inserter)
		}

		for i := 0; i < iters; i++ {
			inserter := inserters[i%len(inserters)]
			insertErr := inserter.Insert(
				now,
				map[string]interface{}{
					"a": i,
					"b": "thing",
				},
				func(cb func(key string, value float64)) {
					cb("val", 1)
				},
			)
			if !assert.NoError(t, insertErr) {
				return
			}
			inserted++
		}

		time.Sleep(5 * time.Second)
	}

	query := func(client rpc.Client, sql string, includeMemStore bool) (*common.QueryMetaData, []*core.FlatRow, error) {
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

	log.Debug("Inserting")
	insert(100)

	queryAndCheck := func(includeMemStore bool) bool {
		time.Sleep(flushLatency / 4)
		for _, client := range clients {
			md, rows, err := query(client, sql, includeMemStore)
			if !assert.NoError(t, err) {
				return false
			}
			if !assert.EqualValues(t, []string{"_points", "val"}, md.FieldNames) {
				return false
			}
			er := testsupport.ExpectedResult{
				testsupport.ExpectedRow{
					now.Truncate(time.Hour).Add(time.Hour),
					map[string]interface{}{
						"b": "thing",
					},
					map[string]float64{
						"_points": float64(inserted),
						"val":     float64(inserted),
					},
				},
			}
			if !er.Assert(t, md, rows) {
				for partition, followersForPartition := range followersByPartition {
					followerClients, _ := clientsForServers(followersForPartition)
					for followerIdx, followerClient := range followerClients {
						_, rows, err := query(followerClient, sql, includeMemStore)
						if err == nil {
							for _, row := range rows {
								t.Logf("Partition: %d   Follower Idx: %d   Points: %f    Val: %f", partition, followerIdx, row.Values[0], row.Values[1])
							}
						}
					}
				}
				return false
			}
		}

		return true
	}

	log.Debug("Checking that unflushed data is available from memstore")
	if !queryAndCheck(true) {
		return
	}

	log.Debug("Waiting to flush and then checking that flushed data is available")
	time.Sleep(flushLatency + 1*time.Second)
	if !queryAndCheck(false) {
		return
	}

	log.Debug("kill redundant followers and make sure we still get the right results from the remaining followers")
	for _, followersForPartition := range followersByPartition {
		if len(followersForPartition) > 1 {
			for i := 1; i < len(followersForPartition); i++ {
				closeServer(followersForPartition[i])
			}
		}
	}
	if !queryAndCheck(false) {
		return
	}

	log.Debug("insert some more")
	insert(100)
	if !queryAndCheck(true) {
		return
	}

	log.Debug("restart leaders and make sure followers reconnect")
	for _, leader := range leaders {
		closeServer(leader)
	}
	leaders, _ = rebuildCluster()
	for _, leader := range leaders {
		startServer(leader)
	}
	clients, err = clientsForServers(leaders)
	if !assert.NoError(t, err) {
		return
	}
	time.Sleep(flushLatency + 1*time.Second)
	if !queryAndCheck(true) {
		return
	}

	log.Debug("restart followers and make sure they connect to existing leaders")
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
	time.Sleep(flushLatency + 1*time.Second)
	if !queryAndCheck(true) {
		return
	}

	log.Debug("kill only the followers that were up while we inserted and make sure that the redundant followers read the data that they missed")
	for _, followersForPartition := range followersByPartition {
		if len(followersForPartition) > 1 {
			closeServer(followersForPartition[0])
		}
	}
	if !queryAndCheck(true) {
		return
	}

	log.Debug("insert just one so that only a single leader is affected (tests that we store a superset of offsets in our data files)")
	insert(1)
	time.Sleep(flushLatency + 1*time.Second)

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
	time.Sleep(flushLatency + 1*time.Second)
	if !queryAndCheck(true) {
		return
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

func clientsForServers(servers []*Server) ([]rpc.Client, error) {
	var clients []rpc.Client
	for _, s := range servers {
		if err := WaitForServer("tcp", s.Addr, 5*time.Second); err != nil {
			return nil, err
		}
		client, err := rpc.Dial(s.Addr, &rpc.ClientOpts{
			Dialer: func(addr string, timeout time.Duration) (net.Conn, error) {
				log.Debugf("Dialing %v with timeout %v", addr, timeout)
				return tls.DialWithDialer(
					&net.Dialer{Timeout: timeout},
					"tcp",
					addr,
					&tls.Config{InsecureSkipVerify: true})
			},
			KeepaliveInterval: 250 * time.Millisecond,
			KeepaliveTimeout:  250 * time.Millisecond,
		})
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	return clients, nil
}