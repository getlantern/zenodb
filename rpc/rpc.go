package rpc

import (
	"context"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/golog"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/planner"
	"google.golang.org/grpc"
)

const (
	PasswordKey = "pwd"
)

var (
	log = golog.LoggerFor("zenodb.rpc")

	Codec = &MsgPackCodec{}
)

type Insert struct {
	Stream       string // note, only the first Insert in a batch needs to include the Stream
	TS           int64
	Dims         []byte
	Vals         []byte
	EndOfInserts bool
}

type InsertReport struct {
	Received  int
	Succeeded int
	Errors    map[int]string
}

type Query struct {
	SQLString       string
	IsSubQuery      bool
	SubQueryResults [][]interface{}
	IncludeMemStore bool
	Unflat          bool
	Deadline        time.Time
	HasDeadline     bool
}

type Point struct {
	Data   []byte
	Offset wal.Offset
}

type RemoteQueryResult struct {
	Fields       core.Fields
	Key          bytemap.ByteMap
	Vals         core.Vals
	Row          *core.FlatRow
	Stats        *common.QueryStats
	Error        string
	EndOfResults bool
}

type RegisterQueryHandler struct {
	Partition int
}

type Client interface {
	NewInserter(ctx context.Context, stream string, opts ...grpc.CallOption) (Inserter, error)

	Query(ctx context.Context, sqlString string, includeMemStore bool, opts ...grpc.CallOption) (*common.QueryMetaData, func(onRow core.OnFlatRow) error, error)

	Follow(ctx context.Context, in *common.Follow, opts ...grpc.CallOption) (func() (data []byte, newOffset wal.Offset, err error), error)

	ProcessRemoteQuery(ctx context.Context, partition int, query planner.QueryClusterFN, timeout time.Duration, opts ...grpc.CallOption) error

	Close() error
}

type Server interface {
	Insert(stream grpc.ServerStream) error

	Query(*Query, grpc.ServerStream) error

	Follow(*common.Follow, grpc.ServerStream) error

	HandleRemoteQueries(r *RegisterQueryHandler, stream grpc.ServerStream) error
}

var ServiceDesc = grpc.ServiceDesc{
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
		{
			StreamName:    "remoteQuery",
			Handler:       remoteQueryHandler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "insert",
			Handler:       insertHandler,
			ClientStreams: true,
		},
	},
}

func insertHandler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(Server).Insert(stream)
}

func queryHandler(srv interface{}, stream grpc.ServerStream) error {
	q := new(Query)
	if err := stream.RecvMsg(q); err != nil {
		return err
	}
	return srv.(Server).Query(q, stream)
}

func followHandler(srv interface{}, stream grpc.ServerStream) error {
	f := new(common.Follow)
	if err := stream.RecvMsg(f); err != nil {
		return err
	}
	return srv.(Server).Follow(f, stream)
}

func remoteQueryHandler(srv interface{}, stream grpc.ServerStream) error {
	r := new(RegisterQueryHandler)
	if err := stream.RecvMsg(r); err != nil {
		return err
	}
	return srv.(Server).HandleRemoteQueries(r, stream)
}
