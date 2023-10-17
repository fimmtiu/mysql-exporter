package main

import (
	"syscall"
	"testing"
	"time"
)

func TestShutdownMonitorTerm(t *testing.T) {
	StartShutdownMonitor()
	controller := NewAsyncController()

	// This background goroutine kills itself after 1 second, then ensures that the ShutdownMonitor noticed it.
	controller.Go(func() error {
		time.Sleep(1 * time.Second)
		syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
		<-controller.ExitSignal()
		return nil
	})

	controller.Wait()
}
