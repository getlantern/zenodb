package rpc

import (
	"time"

	"github.com/getlantern/zenodb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Client interface {
	Query(ctx context.Context, in *Query, opts ...grpc.CallOption) (*zenodb.QueryResult, func() (*zenodb.Row, error), error)

	Close() error
}

func Dial(addr string) (Client, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithInsecure(),
		grpc.WithCodec(msgpackCodec),
		grpc.WithBackoffMaxDelay(1*time.Minute),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()))
	if err != nil {
		return nil, err
	}
	return &client{conn}, nil
}

type client struct {
	cc *grpc.ClientConn
}

func (c *client) Query(ctx context.Context, in *Query, opts ...grpc.CallOption) (*zenodb.QueryResult, func() (*zenodb.Row, error), error) {
	stream, err := grpc.NewClientStream(ctx, &serviceDesc.Streams[0], c.cc, "/zenodb/query", opts...)
	if err != nil {
		return nil, nil, err
	}
	if err := stream.SendMsg(in); err != nil {
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

func (c *client) Close() error {
	return c.cc.Close()
}
