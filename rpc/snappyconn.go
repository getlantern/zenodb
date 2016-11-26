package rpc

import (
	"github.com/golang/snappy"
	"net"
	"time"
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
	w := snappy.NewWriter(conn)
	sc := &snappyConn{Conn: conn, r: r, w: w}
	return sc, nil
}

type snappyConn struct {
	net.Conn
	r *snappy.Reader
	w *snappy.Writer
}

func (sc *snappyConn) Read(p []byte) (int, error) {
	return sc.r.Read(p)
}

func (sc *snappyConn) Write(p []byte) (int, error) {
	return sc.w.Write(p)
}

func (sc *snappyConn) Close() error {
	sc.w.Close()
	return sc.Conn.Close()
}
