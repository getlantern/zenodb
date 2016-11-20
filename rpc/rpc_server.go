package rpc

import (
	"net"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/errors"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/encoding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Server interface {
	Query(string, bool, grpc.ServerStream) error

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

func (s *server) Query(sqlString string, includeMemStore bool, stream grpc.ServerStream) error {
	authorizeErr := s.authorize(stream)
	if authorizeErr != nil {
		return authorizeErr
	}

	query, err := s.db.SQLQuery(sqlString, includeMemStore)
	if err != nil {
		return err
	}

	result, err := query.Run(false)
	if err != nil {
		return err
	}

	rows := result.Rows
	// Clear out result.Rows as we will be streaming these
	result.Rows = nil

	// Send header
	err = stream.SendMsg(result)
	if err != nil {
		return err
	}

	// Stream rows
	for _, row := range rows {
		err = stream.SendMsg(row)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *server) Follow(f *zenodb.Follow, stream grpc.ServerStream) error {
	authorizeErr := s.authorize(stream)
	if authorizeErr != nil {
		return authorizeErr
	}

	log.Debug("Follower joined")
	return s.db.Follow(f, func(data []byte, newOffset wal.Offset) error {
		return stream.SendMsg(&Point{data, newOffset})
	})
}

func (s *server) HandleRemoteQueries(r *RegisterQueryHandler, stream grpc.ServerStream) error {
	initialResultCh := make(chan *RemoteQueryResult)
	initialErrCh := make(chan error)
	finalErrCh := make(chan error)

	s.db.RegisterQueryHandler(r.Partition, func(sqlString string, includeMemStore bool, isSubQuery bool, subQueryResults [][]interface{}, onValue func(bytemap.ByteMap, []encoding.Sequence)) (bool, error) {
		sendErr := stream.SendMsg(&Query{
			SQLString:       sqlString,
			IncludeMemStore: includeMemStore,
			IsSubQuery:      isSubQuery,
			SubQueryResults: subQueryResults,
		})

		m, recvErr := <-initialResultCh, <-initialErrCh

		// Check send error after reading initial result to avoid blocking
		// unnecessarily
		if sendErr != nil {
			return false, errors.New("Unable to send query: %v", sendErr)
		}

		var finalErr error
		hasReadResult := false

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
			onValue(m.Entry.Dims, m.Entry.Vals)
			hasReadResult = true

			// Read next result
			m = &RemoteQueryResult{}
			recvErr = stream.RecvMsg(m)
		}

		finalErrCh <- finalErr
		return hasReadResult, finalErr
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
