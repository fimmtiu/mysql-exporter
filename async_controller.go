// Surprisingly, Go's primitives for managing concurrent processes totally blow. I mean, they're _awful_. This
// class remedies a bit of that by providing a simple way for one task to signal that all the other tasks should
// die, then wait for them all to exit.

package main

import (
	"sync"
)

type AsyncController struct {
	waitGroup sync.WaitGroup
	exitChan chan struct{}
	waitedChan chan struct{}
	err error
}

func NewAsyncController() *AsyncController {
	return &AsyncController{sync.WaitGroup{}, make(chan struct{}), nil, nil}
}

// This registers a new goroutine with the AsyncController and starts it. (Just a wrapper around the 'go'
// keyword.) If the goroutine returns an error, the AsyncController will signal all other goroutines to exit,
// then return the error from Wait().
func (ac *AsyncController) Go(f func() error) {
	ac.waitGroup.Add(1)

	go func() {
		defer ac.waitGroup.Done()
		err := f()
		if err != nil {
			ac.Exit(err)
		}
	}()
}

// This returns a channel which will be readable when it's time for all goroutines to exit. Goroutines started
// by Go() must watch this channel and exit when it's readable.
func (ac *AsyncController) ExitSignal() <-chan struct{} {
	return ac.exitChan
}

// This signals all goroutines to exit. Supply 'err' if the controller is dying because of an error; otherwise,
// just pass nil.
func (ac *AsyncController) Exit(err error) {
	ac.err = err
	ac.waitedChan = make(chan struct{})
	go func(){
		for {
			select {
			case ac.exitChan <- struct{}{}:
				// do nothing, repeat
			case <-ac.waitedChan:
				return
			}
		}
	}()
}

// This waits until all goroutines have exited. Afterwards, the AsyncController is left in a zeroed state where
// it can be re-used. If the caller gave an error to Exit(), this returns the error that they passed. If called
// multiple times, subsequent calls will return nil immediately and do nothing.
func (ac *AsyncController) Wait() error {
	ac.waitGroup.Wait()
	if ac.waitedChan != nil {
		ac.waitedChan <- struct{}{}
		ac.waitedChan = nil
	}

	// Drain any remaining message from exitChan.
	select {
	case <-ac.exitChan:
	default:
	}

	err := ac.err
	ac.err = nil
	return err
}
