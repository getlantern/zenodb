package rpc

import (
	"net"

	"github.com/getlantern/tibsdb"
	"google.golang.org/grpc"
)

type Server interface {
	Query(*Query, grpc.ServerStream) error
}

func Serve(db *tibsdb.DB, l net.Listener) error {
	gs := grpc.NewServer(
		grpc.CustomCodec(msgpackCodec),
		grpc.RPCCompressor(grpc.NewGZIPCompressor()),
		grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	gs.RegisterService(&serviceDesc, &server{db})
	return gs.Serve(l)
}

type server struct {
	db *tibsdb.DB
}

func (s *server) Query(query *Query, stream grpc.ServerStream) error {
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
