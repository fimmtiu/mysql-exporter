package main

import (
	"syscall"
	"testing"
	"time"
)

func TestShutdownMonitorTerm(t *testing.T) {
	StartShutdownMonitor()
	workerGroup := NewWorkerGroup()

	// This background goroutine kills itself after 1 second, then ensures that the ShutdownMonitor noticed it.
	workerGroup.Go(func() error {
		time.Sleep(1 * time.Second)
		syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
		<-workerGroup.ExitSignal()
		return nil
	})

	workerGroup.Wait()
}
