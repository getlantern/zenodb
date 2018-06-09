package rpcserver

import (
	"context"
	"fmt"
	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/golog"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/common"
	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/encoding"
	"github.com/getlantern/zenodb/planner"
	"github.com/getlantern/zenodb/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"net"
	"time"
)

var (
	log = golog.LoggerFor("zenodb.rpc")
)

type Opts struct {
	// Password, if specified, is the password that clients must present in order
	// to access the server.
	Password string
}

// DB is an interface for database-like things (implemented by common.DB).
type DB interface {
	InsertRaw(stream string, ts time.Time, dims bytemap.ByteMap, vals bytemap.ByteMap) error

	Query(sqlString string, isSubQuery bool, subQueryResults [][]interface{}, includeMemStore bool) (core.FlatRowSource, error)

	Follow(f *common.Follow, cb func([]byte, wal.Offset) error)

	RegisterQueryHandler(partition int, query planner.QueryClusterFN)
}

func Serve(db DB, l net.Listener, opts *Opts) error {
	l = &rpc.SnappyListener{l}
	gs := grpc.NewServer(grpc.CustomCodec(rpc.Codec))
	gs.RegisterService(&rpc.ServiceDesc, &server{db, opts.Password})
	return gs.Serve(l)
}

type server struct {
	db       DB
	password string
}

func (s *server) Insert(stream grpc.ServerStream) error {
	// No need to authorize, anyone can insert

	now := time.Now()
	streamName := ""

	report := &rpc.InsertReport{
		Errors: make(map[int]string),
	}

	i := -1
	for {
		i++
		insert := &rpc.Insert{}
		err := stream.RecvMsg(insert)
		if err != nil {
			return fmt.Errorf("Error reading insert: %v", err)
		}
		if insert.EndOfInserts {
			// We're done inserting
			return stream.SendMsg(report)
		}
		report.Received++

		if streamName == "" {
			streamName = insert.Stream
			if streamName == "" {
				return fmt.Errorf("Please specify a stream")
			}
		}

		if len(insert.Dims) == 0 {
			report.Errors[i] = fmt.Sprintf("Need at least one dim")
			continue
		}
		if len(insert.Vals) == 0 {
			report.Errors[i] = fmt.Sprintf("Need at least one val")
			continue
		}
		var ts time.Time
		if insert.TS == 0 {
			ts = now
		} else {
			ts = encoding.TimeFromInt(insert.TS)
		}

		// TODO: make sure we don't barf on invalid bytemaps here
		insertErr := s.db.InsertRaw(streamName, ts, bytemap.ByteMap(insert.Dims), bytemap.ByteMap(insert.Vals))
		if insertErr != nil {
			report.Errors[i] = fmt.Sprintf("Unable to insert: %v", insertErr)
			continue
		}
		report.Succeeded++
	}
}

func (s *server) Query(q *rpc.Query, stream grpc.ServerStream) error {
	authorizeErr := s.authorize(stream)
	if authorizeErr != nil {
		return authorizeErr
	}

	source, err := s.db.Query(q.SQLString, q.IsSubQuery, q.SubQueryResults, q.IncludeMemStore)
	if err != nil {
		return err
	}

	rr := &rpc.RemoteQueryResult{}
	stats, err := source.Iterate(stream.Context(), func(fields core.Fields) error {
		// Send query metadata
		md := zenodb.MetaDataFor(source, fields)
		return stream.SendMsg(md)
	}, func(row *core.FlatRow) (bool, error) {
		rr.Row = row
		return true, stream.SendMsg(rr)
	})
	if err != nil {
		return err
	}

	// Send end of results
	rr.Row = nil
	if stats != nil {
		rr.Stats = stats.(*common.QueryStats)
	}
	rr.EndOfResults = true
	return stream.SendMsg(rr)
}

func (s *server) Follow(f *common.Follow, stream grpc.ServerStream) error {
	authorizeErr := s.authorize(stream)
	if authorizeErr != nil {
		return authorizeErr
	}

	log.Debugf("Follower %d joined", f.PartitionNumber)
	defer log.Debugf("Follower %d left", f.PartitionNumber)
	s.db.Follow(f, func(data []byte, newOffset wal.Offset) error {
		return stream.SendMsg(&rpc.Point{data, newOffset})
	})
	return nil
}

func (s *server) HandleRemoteQueries(r *rpc.RegisterQueryHandler, stream grpc.ServerStream) error {
	initialResultCh := make(chan *rpc.RemoteQueryResult)
	initialErrCh := make(chan error)
	finalErrCh := make(chan error)

	finish := func(err error) {
		select {
		case finalErrCh <- err:
			// ok
		default:
			// ignore
		}
	}

	s.db.RegisterQueryHandler(r.Partition, func(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onFields core.OnFields, onRow core.OnRow, onFlatRow core.OnFlatRow) (interface{}, error) {
		q := &rpc.Query{
			SQLString:       sqlString,
			IsSubQuery:      isSubQuery,
			SubQueryResults: subQueryResults,
			Unflat:          unflat,
			IncludeMemStore: common.ShouldIncludeMemStore(ctx),
		}
		q.Deadline, q.HasDeadline = ctx.Deadline()
		sendErr := stream.SendMsg(q)

		m, recvErr := <-initialResultCh, <-initialErrCh

		// Check send error after reading initial result to avoid blocking
		// unnecessarily
		if sendErr != nil {
			err := errors.New("Unable to send query: %v", sendErr)
			finish(err)
			return nil, err
		}

		var finalErr error

		first := true
	receiveLoop:
		for {
			// Process current result
			if recvErr != nil {
				m.Error = recvErr.Error()
				finalErr = errors.New("Unable to receive result: %v", recvErr)
				break
			}

			log.Debugf("First? %v  Response Error?: %v", first, m.Error)

			if first {
				// First message contains only fields information
				onFields(m.Fields)
				first = false
			} else {
				// Subsequent messages contain data
				if m.EndOfResults {
					break
				}
				var more bool
				var err error
				if unflat {
					more, err = onRow(m.Key, m.Vals)
				} else {
					more, err = onFlatRow(m.Row)
				}
				if !more || err != nil {
					finalErr = err
					break receiveLoop
				}
			}

			// Read next result
			m = &rpc.RemoteQueryResult{}
			recvErr = stream.RecvMsg(m)
		}

		finish(finalErr)
		return m.Stats, finalErr
	})

	// Block on reading initial result to keep connection open
	m := &rpc.RemoteQueryResult{}
	err := stream.RecvMsg(m)
	initialResultCh <- m
	initialErrCh <- err

	if err == nil {
		// Wait for final error so we don't close the connection prematurely
		return <-finalErrCh
	}
	return err
}

func (s *server) authorize(stream grpc.ServerStream) error {
	if s.password == "" {
		log.Debug("No password specified, allowing access to world")
		return nil
	}
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return log.Error("No metadata provided, unable to authenticate")
	}
	passwords := md[rpc.PasswordKey]
	for _, password := range passwords {
		if password == s.password {
			// authorized
			return nil
		}
	}
	return log.Error("None of the provided passwords matched, not authorized!")
}
