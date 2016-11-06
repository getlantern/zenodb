package rpc

import (
	"github.com/getlantern/golog"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"google.golang.org/grpc"
)

const (
	passwordKey = "pwd"
)

var (
	log = golog.LoggerFor("zenodb.rpc")

	msgpackCodec = &MsgPackCodec{}
)

type Query struct {
	SQL string
}

type Point struct {
	Data   []byte
	Offset wal.Offset
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "zenodb",
	HandlerType: (*Server)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "query",
			Handler:       queryHandler,
			ServerStreams: true,
		},
		{
			StreamName:    "follow",
			Handler:       followHandler,
			ServerStreams: true,
		},
	},
}

func queryHandler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Query)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(Server).Query(m, stream)
}

func followHandler(srv interface{}, stream grpc.ServerStream) error {
	m := new(zenodb.Follow)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(Server).Follow(m, stream)
}
