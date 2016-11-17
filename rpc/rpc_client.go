package rpc

import (
	"io"
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
}

type Client interface {
	Query(ctx context.Context, sqlString string, opts ...grpc.CallOption) (*zenodb.QueryResult, func() (*zenodb.Row, error), error)

	Follow(ctx context.Context, in *zenodb.Follow, opts ...grpc.CallOption) (func() (data []byte, newOffset wal.Offset, err error), error)

	ProcessRemoteQuery(ctx context.Context, partition int, query func(sqlString string, isSubQuery bool, subQueryResults [][]interface{}, onEntry func(*zenodb.Entry) error) error, opts ...grpc.CallOption) error

	Close() error
}

func Dial(addr string, opts *ClientOpts) (Client, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithInsecure(),
		grpc.WithCodec(msgpackCodec),
		grpc.WithBackoffMaxDelay(1*time.Minute),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()))
	if err != nil {
		return nil, err
	}
	return &client{conn, opts.Password}, nil
}

type client struct {
	cc       *grpc.ClientConn
	password string
}

func (c *client) Query(ctx context.Context, sqlString string, opts ...grpc.CallOption) (*zenodb.QueryResult, func() (*zenodb.Row, error), error) {
	stream, err := grpc.NewClientStream(c.authenticated(ctx), &serviceDesc.Streams[0], c.cc, "/zenodb/query", opts...)
	if err != nil {
		return nil, nil, err
	}
	if err := stream.SendMsg(&Query{sqlString}); err != nil {
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

func (c *client) SubQuery(ctx context.Context, sqlString string, opts ...grpc.CallOption) ([]interface{}, error) {
	stream, err := grpc.NewClientStream(c.authenticated(ctx), &serviceDesc.Streams[0], c.cc, "/zenodb/subquery", opts...)
	if err != nil {
		return nil, err
	}
	if err := stream.SendMsg(&Query{sqlString}); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}

	var result []interface{}
	return result, stream.RecvMsg(&result)
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

func (c *client) ProcessRemoteQuery(ctx context.Context, partition int, query func(sqlString string, isSubQuery bool, subQueryResults [][]interface{}, onEntry func(*zenodb.Entry) error) error, opts ...grpc.CallOption) error {
	stream, err := grpc.NewClientStream(c.authenticated(ctx), &serviceDesc.Streams[2], c.cc, "/zenodb/remoteQuery", opts...)
	if err != nil {
		return errors.New("Unable to obtain client stream: %v", err)
	}
	defer stream.CloseSend()

	if err := stream.SendMsg(&RegisterQueryHandler{partition}); err != nil {
		return errors.New("Unable to send registration message: %v", err)
	}

	q := &RemoteQuery{}
	recvErr := stream.RecvMsg(q)
	if recvErr != nil {
		return errors.New("Unable to read query: %v", recvErr)
	}
	queryErr := query(q.SQLString, q.IsSubQuery, q.SubQueryResults, func(entry *zenodb.Entry) error {
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
