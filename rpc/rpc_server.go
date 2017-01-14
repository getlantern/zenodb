package rpc

import (
	"context"
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"net"
)

type Server interface {
	Query(*Query, grpc.ServerStream) error

	Follow(*zenodb.Follow, grpc.ServerStream) error

	HandleRemoteQueries(r *RegisterQueryHandler, stream grpc.ServerStream) error
}

type ServerOpts struct {
	// Password, if specified, is the password that clients must present in order
	// to access the server.
	Password string
}

func Serve(db *zenodb.DB, l net.Listener, opts *ServerOpts) error {
	l = &snappyListener{l}
	gs := grpc.NewServer(
		grpc.CustomCodec(msgpackCodec))
	gs.RegisterService(&serviceDesc, &server{db, opts.Password})
	return gs.Serve(l)
}

type server struct {
	db       *zenodb.DB
	password string
}

func (s *server) Query(q *Query, stream grpc.ServerStream) error {
	authorizeErr := s.authorize(stream)
	if authorizeErr != nil {
		return authorizeErr
	}

	source, err := s.db.Query(q.SQLString, q.IsSubQuery, q.SubQueryResults, q.IncludeMemStore)
	if err != nil {
		return err
	}

	// Send query metadata
	md := zenodb.MetaDataFor(source)
	err = stream.SendMsg(md)
	if err != nil {
		return err
	}

	rr := &RemoteQueryResult{}
	err = source.Iterate(stream.Context(), func(row *core.FlatRow) (bool, error) {
		rr.Row = row
		return true, stream.SendMsg(rr)
	})
	if err != nil {
		return err
	}

	// Send end of results
	rr.Row = nil
	rr.EndOfResults = true
	return stream.SendMsg(rr)
}

func (s *server) Follow(f *zenodb.Follow, stream grpc.ServerStream) error {
	authorizeErr := s.authorize(stream)
	if authorizeErr != nil {
		return authorizeErr
	}

	log.Debugf("Follower %d joined", f.PartitionNumber)
	defer log.Debugf("Follower %d left", f.PartitionNumber)
	return s.db.Follow(f, func(data []byte, newOffset wal.Offset) error {
		return stream.SendMsg(&Point{data, newOffset})
	})
}

func (s *server) HandleRemoteQueries(r *RegisterQueryHandler, stream grpc.ServerStream) error {
	initialResultCh := make(chan *RemoteQueryResult)
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

	s.db.RegisterQueryHandler(r.Partition, func(ctx context.Context, sqlString string, isSubQuery bool, subQueryResults [][]interface{}, unflat bool, onRow core.OnRow, onFlatRow core.OnFlatRow) error {
		sendErr := stream.SendMsg(&Query{
			SQLString:       sqlString,
			IsSubQuery:      isSubQuery,
			SubQueryResults: subQueryResults,
			Unflat:          unflat,
		})

		m, recvErr := <-initialResultCh, <-initialErrCh

		// Check send error after reading initial result to avoid blocking
		// unnecessarily
		if sendErr != nil {
			err := errors.New("Unable to send query: %v", sendErr)
			finish(err)
			return err
		}

		var finalErr error

		for {
			// Process current result
			if recvErr != nil {
				m.Error = recvErr.Error()
				finalErr = errors.New("Unable to receive result: %v", recvErr)
				break
			}
			if m.EndOfResults {
				break
			}
			if unflat {
				onRow(m.Key, m.Vals)
			} else {
				onFlatRow(m.Row)
			}

			// Read next result
			m = &RemoteQueryResult{}
			recvErr = stream.RecvMsg(m)
		}

		finish(finalErr)
		return finalErr
	})

	// Block on reading initial result to keep connection open
	m := &RemoteQueryResult{}
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
	md, ok := metadata.FromContext(stream.Context())
	if !ok {
		return log.Error("No metadata provided, unable to authenticate")
	}
	passwords := md[passwordKey]
	for _, password := range passwords {
		if password == s.password {
			// authorized
			return nil
		}
	}
	return log.Error("None of the provided passwords matched, not authorized!")
}
