// Surprisingly, Go's primitives for managing concurrent processes totally blow. I mean, they're _awful_. This
// class remedies a bit of that by providing a simple way for one task to signal that all the other tasks should
// die, then wait for them all to exit.

package main

import (
	"sync"
	"sync/atomic"
)

type WorkerGroup struct {
	waitGroup sync.WaitGroup
	exitChan chan struct{}
	doneChan chan struct{}
	count atomic.Int32
	err error
}

func NewWorkerGroup() *WorkerGroup {
	return &WorkerGroup{sync.WaitGroup{}, make(chan struct{}), make(chan struct{}), atomic.Int32{}, nil}
}

// This registers a new goroutine with the WorkerGroup and starts it. (Just a wrapper around the 'go'
// keyword.) If the goroutine returns an error, the WorkerGroup will signal all other goroutines to exit,
// then return the error from Wait().
func (ac *WorkerGroup) Go(fn func() error) {
	ac.waitGroup.Add(1)
	ac.count.Add(1)

	go func() {
		defer func() {
			ac.waitGroup.Done()
			if ac.count.Add(-1) == 0 {
				close(ac.doneChan)
			}
		}()

		err := fn()
		if err != nil {
			ac.Exit(err)
		}
	}()
}

// This returns a channel which will close when all goroutines have naturally exited.
func (ac *WorkerGroup) DoneSignal() <-chan struct{} {
	return ac.doneChan
}

// This returns a channel which will be closed when it's time for all goroutines to exit. Goroutines started
// by Go() must watch this channel and exit when it's closed.
func (ac *WorkerGroup) ExitSignal() <-chan struct{} {
	return ac.exitChan
}

// This signals all goroutines to exit. Supply 'err' if the group is dying because of an error; otherwise,
// just pass nil.
func (ac *WorkerGroup) Exit(err error) {
	ac.err = err
	close(ac.exitChan)
}

// This blocks until all goroutines have exited. If the caller gave an error to Exit(), this returns the error
// that they passed. If called multiple times, subsequent calls will return nil immediately and do nothing.
func (ac *WorkerGroup) Wait() error {
	ac.waitGroup.Wait()
	err := ac.err
	ac.err = nil
	return err
}
