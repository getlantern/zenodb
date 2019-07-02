package rpcserver

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/planner"
	"github.com/getlantern/zenodb/rpc"
	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if !assert.NoError(t, err) {
		return
	}
	defer l.Close()

	db := &mockDB{}
	start, _ := PrepareServer(db, l, &Opts{
		Password: "password",
	})
	go start()
	time.Sleep(1 * time.Second)

	client, err := rpc.Dial(l.Addr().String(), &rpc.ClientOpts{
		Password: "password",
		Dialer: func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("tcp", addr, timeout)
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	defer client.Close()

	inserter, err := client.NewInserter(context.Background(), "thestream")
	if !assert.NoError(t, err) {
		return
	}

	for i := 0; i < 10; i++ {
		dims := map[string]interface{}{"dim": "dimval"}
		if i > 1 && i < 7 {
			dims = nil
		}
		err = inserter.Insert(time.Time{}, dims, func(cb func(key string, value float64)) {
			if i < 7 {
				cb("val", float64(i))
			}
		})
		if !assert.NoError(t, err, "Error on iteration %d", i) {
			return
		}
	}

	report, err := inserter.Close()
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, 10, report.Received)
	assert.Equal(t, 2, report.Succeeded)
	assert.Equal(t, 2, db.NumInserts())
	for i := 2; i < 10; i++ {
		if i < 7 {
			assert.Equal(t, "Need at least one dim", report.Errors[i])
		} else {
			assert.Equal(t, "Need at least one val", report.Errors[i])
		}
	}
}

type mockDB struct {
	numInserts int64
}

func (db *mockDB) InsertRaw(stream string, ts time.Time, dims bytemap.ByteMap, vals bytemap.ByteMap) error {
	atomic.AddInt64(&db.numInserts, 1)
	return nil
}

func (db *mockDB) NumInserts() int {
	return int(atomic.LoadInt64(&db.numInserts))
}

func (db *mockDB) Query(sqlString string, isSubQuery bool, subQueryResults [][]interface{}, includeMemStore bool) (core.FlatRowSource, error) {
	return nil, nil
}

func (db *mockDB) Follow(f *common.Follow, cb func([]byte, wal.Offset) error) {
}

func (db *mockDB) RegisterQueryHandler(partition int, query planner.QueryClusterFN) {

}
