package kafka

import (
	"os"
	"os/signal"
	"syscall"
)

var sigchan = make(chan os.Signal, 1)

//ShutdownManager manages the shutdown of the consumer and producer
type ShutdownManager struct {
	ShutdownState bool
}

//NewShutdownManager manages the shutdown
func NewShutdownManager() *ShutdownManager {
	return &ShutdownManager{}
}

//EnableSignalMonitor watches sigint and sigterm and triggers the shutdown for all consumer and producer
func (s *ShutdownManager) EnableSignalMonitor() {
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigchan
		s.SignalShutdown()
	}()
}

//SignalShutdown triggers the shutdown for all consumer and producer
func (s *ShutdownManager) SignalShutdown() {
	s.ShutdownState = true
}
