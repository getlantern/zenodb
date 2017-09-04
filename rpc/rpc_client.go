package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/mtime"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/planner"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientOpts struct {
	// Password, if specified, is the password that client will present to server
	// in order to gain access.
	Password string

	Dialer func(string, time.Duration) (net.Conn, error)
}

type Inserter interface {
	Insert(ts time.Time, dims map[string]interface{}, vals func(func(string, interface{}))) error

	Close() (*InsertReport, error)
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
		grpc.WithCodec(Codec),
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

type inserter struct {
	clientStream grpc.ClientStream
	streamName   string
}

func (c *client) NewInserter(ctx context.Context, streamName string, opts ...grpc.CallOption) (Inserter, error) {
	clientStream, err := grpc.NewClientStream(ctx, &ServiceDesc.Streams[3], c.cc, "/zenodb/insert", opts...)
	if err != nil {
		return nil, err
	}

	return &inserter{
		clientStream: clientStream,
		streamName:   streamName,
	}, nil
}

func (i *inserter) Insert(ts time.Time, dims map[string]interface{}, vals func(func(string, interface{}))) error {
	insert := &Insert{
		Stream: i.streamName,
		TS:     ts.UnixNano(),
		Dims:   bytemap.New(dims),
		Vals:   bytemap.Build(vals, nil, true),
	}
	// Set streamName to "" to prevent sending it unnecessarily in subsequent inserts
	i.streamName = ""
	return i.clientStream.SendMsg(insert)
}

func (i *inserter) Close() (*InsertReport, error) {
	err := i.clientStream.SendMsg(&Insert{EndOfInserts: true})
	if err != nil {
		return nil, fmt.Errorf("Unable to send closing message: %v", err)
	}
	err = i.clientStream.CloseSend()
	if err != nil {
		return nil, fmt.Errorf("Unable to close send: %v", err)
	}
	report := &InsertReport{}
	err = i.clientStream.RecvMsg(&report)
	if err != nil {
		return nil, fmt.Errorf("Error from server: %v", err)
	}
	return report, nil
}

func (c *client) Query(ctx context.Context, sqlString string, includeMemStore bool, opts ...grpc.CallOption) (*common.QueryMetaData, func(onRow core.OnFlatRow) error, error) {
	stream, err := grpc.NewClientStream(c.authenticated(ctx), &ServiceDesc.Streams[0], c.cc, "/zenodb/query", opts...)
	if err != nil {
		return nil, nil, err
	}
	if err = stream.SendMsg(&Query{SQLString: sqlString, IncludeMemStore: includeMemStore}); err != nil {
		return nil, nil, err
	}
	if err = stream.CloseSend(); err != nil {
		return nil, nil, err
	}

	md := &common.QueryMetaData{}
	err = stream.RecvMsg(md)
	if err != nil {
		return nil, nil, err
	}

	iterate := func(onRow core.OnFlatRow) error {
		for {
			result := &RemoteQueryResult{}
			rowErr := stream.RecvMsg(result)
			if rowErr != nil {
				return rowErr
			}
			if result.EndOfResults {
				return nil
			}
			more, rowErr := onRow(result.Row)
			if !more || rowErr != nil {
				return rowErr
			}
		}
	}

	return md, iterate, nil
}

func (c *client) Follow(ctx context.Context, f *common.Follow, opts ...grpc.CallOption) (func() (data []byte, newOffset wal.Offset, err error), error) {
	stream, err := grpc.NewClientStream(c.authenticated(ctx), &ServiceDesc.Streams[1], c.cc, "/zenodb/follow", opts...)
	if err != nil {
		return nil, err
	}
	if err := stream.SendMsg(f); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}

	next := func() ([]byte, wal.Offset, error) {
		point := &Point{}
		err := stream.RecvMsg(point)
		if err != nil {
			return nil, nil, err
		}
		return point.Data, point.Offset, nil
	}

	return next, nil
}

func (c *client) ProcessRemoteQuery(ctx context.Context, partition int, query planner.QueryClusterFN, opts ...grpc.CallOption) error {
	elapsed := mtime.Stopwatch()
	defer func() {
		log.Debugf("Finished processing query in %v", elapsed())
	}()

	stream, err := grpc.NewClientStream(c.authenticated(ctx), &ServiceDesc.Streams[2], c.cc, "/zenodb/remoteQuery", opts...)
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

	onFields := func(fields core.Fields) error {
		return stream.SendMsg(&RemoteQueryResult{Fields: fields})
	}
	var onRow core.OnRow
	var onFlatRow core.OnFlatRow

	if q.Unflat {
		onRow = func(key bytemap.ByteMap, vals core.Vals) (bool, error) {
			err := stream.SendMsg(&RemoteQueryResult{Key: key, Vals: vals})
			return true, err
		}
	} else {
		onFlatRow = func(row *core.FlatRow) (bool, error) {
			err := stream.SendMsg(&RemoteQueryResult{Row: row})
			return true, err
		}
	}

	streamCtx := stream.Context()
	if q.HasDeadline {
		var cancel context.CancelFunc
		streamCtx, cancel = context.WithDeadline(streamCtx, q.Deadline)
		defer cancel()
	}
	streamCtx = common.WithIncludeMemStore(streamCtx, q.IncludeMemStore)

	queryErr := query(streamCtx, q.SQLString, q.IsSubQuery, q.SubQueryResults, q.Unflat, onFields, onRow, onFlatRow)
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
	md := metadata.New(map[string]string{PasswordKey: c.password})
	return metadata.NewOutgoingContext(ctx, md)
}
