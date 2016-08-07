package rpc

import (
	"github.com/getlantern/golog"
	"google.golang.org/grpc"
)

var (
	log = golog.LoggerFor("rpc")

	msgpackCodec = &MsgPackCodec{}
)

type Query struct {
	SQL string
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
	},
}

func queryHandler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Query)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(Server).Query(m, stream)
}
