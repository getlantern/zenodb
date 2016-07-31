package rpc

import (
	"google.golang.org/grpc"
)

var msgpackCodec = &MsgPackCodec{}

type Query struct {
	SQL string
}

type Row struct {
	Dims   []interface{}
	Fields [][]float64
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "TibsDB",
	HandlerType: (*Server)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Query",
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
