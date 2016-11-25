package rpc

import (
	"github.com/golang/snappy"
	"net"
	"sync"
	"time"
)

var (
	flushInterval = 1 * time.Second
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
		time.Sleep(flushInterval)
		sc.flushMx.Lock()
		err := sc.w.Flush()
		if err != nil {
			sc.flushMx.Unlock()
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

func (sc *snappyConn) Close() error {
	sc.flushMx.Lock()
	flushErr := sc.w.Close()
	sc.flushMx.Unlock()
	closeErr := sc.Conn.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
