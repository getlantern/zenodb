package server

import (
	"os"
	"os/signal"
	"syscall"
)

func (s *Server) HandleShutdownSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-c
		s.log.Debugf("Got signal \"%s\", closing db and exiting...", sig)
		s.Close()
	}()
}
