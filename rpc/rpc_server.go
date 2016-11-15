package rpc

import (
	"errors"
	"net"
	"sync"

	"github.com/getlantern/bytemap"
	"github.com/getlantern/wal"
	"github.com/getlantern/zenodb"
	"github.com/getlantern/zenodb/encoding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Server interface {
	Query(string, grpc.ServerStream) error

	Follow(*zenodb.Follow, grpc.ServerStream) error

	HandleRemoteQueries(r *zenodb.RegisterQueryHandler, stream grpc.ServerStream) error
}

type ServerOpts struct {
	// Password, if specified, is the password that clients must present in order
	// to access the server.
	Password string
}

func Serve(db *zenodb.DB, l net.Listener, opts *ServerOpts) error {
	gs := grpc.NewServer(
		grpc.CustomCodec(msgpackCodec),
		grpc.RPCCompressor(grpc.NewGZIPCompressor()),
		grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	gs.RegisterService(&serviceDesc, &server{db, opts.Password})
	return gs.Serve(l)
}

type server struct {
	db       *zenodb.DB
	password string
}

func (s *server) Query(sqlString string, stream grpc.ServerStream) error {
	authorizeErr := s.authorize(stream)
	if authorizeErr != nil {
		return authorizeErr
	}

	query, err := s.db.SQLQuery(sqlString)
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

func (s *server) HandleRemoteQueries(r *zenodb.RegisterQueryHandler, stream grpc.ServerStream) error {
	var mx sync.RWMutex
	id := 0
	results := make(map[int]chan *RemoteQueryRelated)
	s.db.RegisterQueryHandler(r, func(sqlString string, isSubQuery bool, subQueryResults [][]interface{}, onValue func(bytemap.ByteMap, []encoding.Sequence)) error {
		resultCh := make(chan *RemoteQueryRelated)
		mx.Lock()
		qid := id
		results[qid] = resultCh
		id++
		mx.Unlock()
		defer func() {
			mx.Lock()
			delete(results, qid)
			mx.Unlock()
		}()

		stream.SendMsg(&RemoteQueryRelated{
			ID:              qid,
			SQLString:       sqlString,
			IsSubQuery:      isSubQuery,
			SubQueryResults: subQueryResults,
		})
		for result := range resultCh {
			if result.Error != "" {
				return errors.New(result.Error)
			}
			onValue(result.Entry.Dims, result.Entry.Vals)
		}
		return nil
	})

	for {
		m := &RemoteQueryRelated{}
		recvErr := stream.RecvMsg(m)
		if recvErr != nil {
			m.Error = recvErr.Error()
			mx.RLock()
			defer mx.RUnlock()
			for _, resultCh := range results {
				resultCh <- m
				close(resultCh)
			}
			return recvErr
		}
		mx.RLock()
		resultCh := results[m.ID]
		mx.RUnlock()
		if resultCh == nil {
			log.Errorf("Received result for unknown id: %d", id)
			continue
		}
		if m.EndOfResults {
			close(resultCh)
			continue
		}
		resultCh <- m
	}
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
