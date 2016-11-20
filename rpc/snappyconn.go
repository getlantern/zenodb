package rpc

import (
	"github.com/golang/snappy"
	"net"
	"sync"
	"time"
)

var (
	flushInterval = 50 * time.Millisecond
)

func snappyDialer(d func(string, time.Duration) (net.Conn, error)) func(addr string, timeout time.Duration) (net.Conn, error) {
	return func(addr string, timeout time.Duration) (net.Conn, error) {
		return snappyWrap(d(addr, timeout))
	}
}

type snappyListener struct {
	net.Listener
}

func (sl *snappyListener) Accept() (net.Conn, error) {
	return snappyWrap(sl.Listener.Accept())
}

func snappyWrap(conn net.Conn, err error) (net.Conn, error) {
	if err != nil {
		return nil, err
	}
	r := snappy.NewReader(conn)
	w := snappy.NewBufferedWriter(conn)
	sc := &snappyConn{Conn: conn, r: r, w: w}
	go sc.flushPeriodically()
	return sc, nil
}

type snappyConn struct {
	net.Conn
	r       *snappy.Reader
	w       *snappy.Writer
	flushMx sync.RWMutex
}

func (sc *snappyConn) flushPeriodically() {
	for {
		time.Sleep(50 * time.Millisecond)
		sc.flushMx.Lock()
		err := sc.w.Flush()
		if err != nil {
			return
		}
		sc.flushMx.Unlock()
	}
}

func (sc *snappyConn) Read(p []byte) (int, error) {
	return sc.r.Read(p)
}

func (sc *snappyConn) Write(p []byte) (int, error) {
	sc.flushMx.RLock()
	n, err := sc.w.Write(p)
	sc.flushMx.RUnlock()
	return n, err
}
