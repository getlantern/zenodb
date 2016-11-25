package rpc

import (
	"io"
	"net"
	"time"

	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientOpts struct {
	// Password, if specified, is the password that client will present to server
	// in order to gain access.
	Password string

	Dialer func(string, time.Duration) (net.Conn, error)
}

type Client interface {
	Query(ctx context.Context, sqlString string, includeMemStore bool, opts ...grpc.CallOption) (*zenodb.QueryResult, func() (*zenodb.Row, error), error)

	Follow(ctx context.Context, in *zenodb.Follow, opts ...grpc.CallOption) (func() (data []byte, newOffset wal.Offset, err error), error)

	ProcessRemoteQuery(ctx context.Context, partition int, query func(sqlString string, includeMemStore bool, isSubQuery bool, subQueryResults [][]interface{}, onEntry func(*zenodb.Entry) error) error, opts ...grpc.CallOption) error

	Close() error
}

func Dial(addr string, opts *ClientOpts) (Client, error) {
	if opts.Dialer == nil {
		// Use default dialer
		opts.Dialer = func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("tcp", addr, timeout)
		}
	}

	opts.Dialer = snappyDialer(opts.Dialer)

	conn, err := grpc.Dial(addr,
		grpc.WithInsecure(),
		grpc.WithDialer(opts.Dialer),
		grpc.WithCodec(msgpackCodec),
		grpc.WithBackoffMaxDelay(1*time.Minute))
	if err != nil {
		return nil, err
	}
	return &client{conn, opts.Password}, nil
}

type client struct {
	cc       *grpc.ClientConn
	password string
}

func (c *client) Query(ctx context.Context, sqlString string, includeMemStore bool, opts ...grpc.CallOption) (*zenodb.QueryResult, func() (*zenodb.Row, error), error) {
	stream, err := grpc.NewClientStream(c.authenticated(ctx), &serviceDesc.Streams[0], c.cc, "/zenodb/query", opts...)
	if err != nil {
		return nil, nil, err
	}
	if err := stream.SendMsg(&Query{SQLString: sqlString, IncludeMemStore: includeMemStore}); err != nil {
		return nil, nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, nil, err
	}

	result := &zenodb.QueryResult{}
	err = stream.RecvMsg(result)
	if err != nil {
		return nil, nil, err
	}

	nextRow := func() (*zenodb.Row, error) {
		row := &zenodb.Row{}
		err := stream.RecvMsg(row)
		return row, err
	}
	return result, nextRow, nil
}

func (c *client) Follow(ctx context.Context, f *zenodb.Follow, opts ...grpc.CallOption) (func() (data []byte, newOffset wal.Offset, err error), error) {
	stream, err := grpc.NewClientStream(c.authenticated(ctx), &serviceDesc.Streams[1], c.cc, "/zenodb/follow", opts...)
	if err != nil {
		return nil, err
	}
	if err := stream.SendMsg(f); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}

	point := &Point{}
	next := func() ([]byte, wal.Offset, error) {
		err := stream.RecvMsg(point)
		if err != nil {
			return nil, nil, err
		}
		return point.Data, point.Offset, nil
	}

	return next, nil
}

func (c *client) ProcessRemoteQuery(ctx context.Context, partition int, query func(sqlString string, includeMemStore bool, isSubQuery bool, subQueryResults [][]interface{}, onEntry func(*zenodb.Entry) error) error, opts ...grpc.CallOption) error {
	stream, err := grpc.NewClientStream(c.authenticated(ctx), &serviceDesc.Streams[2], c.cc, "/zenodb/remoteQuery", opts...)
	if err != nil {
		return errors.New("Unable to obtain client stream: %v", err)
	}
	defer stream.CloseSend()

	if err := stream.SendMsg(&RegisterQueryHandler{partition}); err != nil {
		return errors.New("Unable to send registration message: %v", err)
	}

	q := &Query{}
	recvErr := stream.RecvMsg(q)
	if recvErr != nil {
		return errors.New("Unable to read query: %v", recvErr)
	}
	if q.SQLString == "" {
		// It's a noop query, ignore
		return nil
	}
	queryErr := query(q.SQLString, q.IncludeMemStore, q.IsSubQuery, q.SubQueryResults, func(entry *zenodb.Entry) error {
		return stream.SendMsg(&RemoteQueryResult{Entry: entry})
	})
	result := &RemoteQueryResult{EndOfResults: true}
	if queryErr != nil && queryErr != io.EOF {
		result.Error = queryErr.Error()
	}
	stream.SendMsg(result)

	return nil
}

func (c *client) Close() error {
	return c.cc.Close()
}

func (c *client) authenticated(ctx context.Context) context.Context {
	if c.password == "" {
		return ctx
	}
	md := metadata.New(map[string]string{passwordKey: c.password})
	return metadata.NewContext(ctx, md)
}
