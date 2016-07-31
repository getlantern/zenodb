package rpc

import (
	"github.com/getlantern/tibsdb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Client interface {
	Query(ctx context.Context, in *Query, opts ...grpc.CallOption) (*tibsdb.QueryResult, func() (*Row, error), error)

	Close() error
}

func Dial(addr string) (Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithCodec(msgpackCodec))
	if err != nil {
		return nil, err
	}
	return &tibsDBClient{conn}, nil
}

type tibsDBClient struct {
	cc *grpc.ClientConn
}

func (c *tibsDBClient) Query(ctx context.Context, in *Query, opts ...grpc.CallOption) (*tibsdb.QueryResult, func() (*Row, error), error) {
	stream, err := grpc.NewClientStream(ctx, &serviceDesc.Streams[0], c.cc, "/TibsDB/Query", opts...)
	if err != nil {
		return nil, nil, err
	}
	if err := stream.SendMsg(in); err != nil {
		return nil, nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, nil, err
	}

	result := &tibsdb.QueryResult{}
	err = stream.RecvMsg(result)
	if err != nil {
		return nil, nil, err
	}

	nextRow := func() (*Row, error) {
		row := &Row{}
		err := stream.RecvMsg(row)
		return row, err
	}
	return result, nextRow, nil
}

func (c *tibsDBClient) Close() error {
	return c.cc.Close()
}
