package rpc

import (
	"net"

	"github.com/getlantern/zenodb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Server interface {
	Query(*Query, grpc.ServerStream) error
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

func (s *server) Query(query *Query, stream grpc.ServerStream) error {
	authorizeErr := s.authorize(stream)
	if authorizeErr != nil {
		return authorizeErr
	}

	q, err := s.db.SQLQuery(query.SQL)
	if err != nil {
		return err
	}
	result, err := q.Run()
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
