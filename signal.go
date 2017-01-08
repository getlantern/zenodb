package zenodb

import (
	"os"
	"os/signal"
	"syscall"
)

func (db *DB) HandleShutdownSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		s := <-c
		log.Debugf("Got signal \"%s\", exiting...", s)
		db.Close()
		os.Exit(0)
	}()
}
