package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorkerGroupWait(t *testing.T) {
	ac := NewWorkerGroup()
	ac.Go(func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	ac.Go(func() error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	ac.Go(func() error {
		time.Sleep(300 * time.Millisecond)
		return nil
	})
	ac.Wait()
}

func TestWorkerGroupExit(t *testing.T) {
	ac := NewWorkerGroup()
	ac.Go(func() error {
		<-ac.ExitSignal()
		return nil
	})
	ac.Go(func() error {
		<-ac.ExitSignal()
		return nil
	})
	ac.Go(func() error {
		<-ac.ExitSignal()
		return nil
	})
	ac.Exit(nil)
	ac.Wait()

	if _, ok := <-ac.exitChan; ok {
		t.Error("exitChan should be closed after Exit() is called.")
	}
}

func TestWorkerGroupError(t *testing.T) {
	ac := NewWorkerGroup()
	ac.Go(func() error {
		<-ac.ExitSignal()
		return nil
	})
	ac.Exit(errors.New("HONK"))
	err := ac.Wait()

	assert.Equal(t, err.Error(), "HONK")
}

func TestWorkerGroupMultipleWait(t *testing.T) {
	ac := NewWorkerGroup()
	ac.Go(func() error {
		<-ac.ExitSignal()
		return nil
	})
	ac.Exit(errors.New("HONK"))
	ac.Wait()
	ac.Wait()
	ac.Wait()
}

func TestWorkerGroupDone(t *testing.T) {
	ac := NewWorkerGroup()
	ac.Go(func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	select {
	case <-ac.DoneSignal():
	}
}

func TestWorkerGroupExitsOnGoroutineError(t *testing.T) {
	ac := NewWorkerGroup()
	ac.Go(func() error {
		<-ac.ExitSignal()
		return nil
	})
	ac.Go(func() error {
		return errors.New("HONK")
	})

	err := ac.Wait()
	assert.Equal(t, err.Error(), "HONK")
}
