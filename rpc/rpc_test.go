package rpc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/planner"
	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if !assert.NoError(t, err) {
		return
	}
	defer l.Close()

	db := &mockDB{}
	go func() {
		err = Serve(db, l, &ServerOpts{
			Password: "password",
		})
		assert.NoError(t, err)
	}()
	time.Sleep(1 * time.Second)

	client, err := Dial(l.Addr().String(), &ClientOpts{
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
		err = inserter.Insert(time.Time{}, map[string]interface{}{"dim": "dimval"}, func(cb func(key string, value interface{})) {
			cb("val", float64(i))
		})
		if !assert.NoError(t, err, "Error on iteration %d", i) {
			return
		}
	}

	err = inserter.Close()
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, 10, db.NumInserts())
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

func (db *mockDB) Follow(f *zenodb.Follow, cb func([]byte, wal.Offset) error) error {
	return nil
}

func (db *mockDB) RegisterQueryHandler(partition int, query planner.QueryClusterFN) {

}
