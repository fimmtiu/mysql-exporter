package main

import (
	"fmt"
	"math"
	"time"
)

const MAX_RETRIES = 10   // Picked this number out of the air. Let's revisit this later.

type Snapshotter struct {
	State SnapshotState
	Workers *WorkerGroup
	PendingIntervalsChan chan PendingInterval
	CompletedIntervalsChan chan PendingInterval
	ExitChan chan struct{}
}

func NewSnapshotter() *Snapshotter {
	tables, err := ListTables()
	if err != nil {
		panic(err)
	}
	return NewCustomSnapshotter(NewSnapshotState(tables))
}

func NewCustomSnapshotter(state SnapshotState) *Snapshotter {
	return &Snapshotter{
		state,
		NewWorkerGroup(),
		make(chan PendingInterval),
		make(chan PendingInterval),
		make(chan struct{}),
	}
}

// Returns true if we should keep going and false if we should exit.
func (s *Snapshotter) Run() bool {
	successfulExit := true
	if s.State.Done() {
		return true
	}

	for i := 0; i < int(config.SnapshotWorkers); i++ {
		s.Workers.Go(s.runWorker)
	}
	logger.Printf("Started %d snapshot workers.", config.SnapshotWorkers)

	nextInterval, ok := s.State.GetNextPendingInterval()
	if !ok {
		panic(fmt.Errorf("No pending intervals at the start of the snapshot?"))
	}

	pendingChan := s.PendingIntervalsChan
	loop: for {
		select {
		case pendingChan <- nextInterval:
			nextInterval, ok = s.State.GetNextPendingInterval()
			if !ok {
				close(s.PendingIntervalsChan)
				pendingChan = nil
			}
		case completedInterval := <- s.CompletedIntervalsChan:
			err := s.State.MarkIntervalDone(completedInterval)
			if err != nil {
				panic(err)
			}
		case <-s.ExitChan:
			logger.Printf("Signalling all workers to exit.")
			s.Workers.Exit(nil)
			s.ExitChan = nil
			successfulExit = false
		case <-s.Workers.DoneSignal():
			logger.Printf("The snapshot is complete.")
			break loop
		}
	}
	s.Workers.Wait()
	return successfulExit
}

func (s *Snapshotter) Exit() {
	close(s.ExitChan)
}

func (s *Snapshotter) runWorker() error {
	for {
		select {
		case pi, ok := <-s.PendingIntervalsChan:
			if !ok {
				return nil
			}

			result, err := getRowChunk(pi)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Row number: %d\n", result.RowNumber())
			for row := 0; row < result.RowNumber(); row++ {

			}

			s.CompletedIntervalsChan <- pi

		case <-s.Workers.ExitSignal():
			return nil
		}
	}
}

func getRowChunk(pi PendingInterval) (IMysqlResult, error) {
	retries := 0
	var result IMysqlResult
	var err error

	for retries < MAX_RETRIES {
		sql := fmt.Sprintf("SELECT * FROM `%s` WHERE `id` >= %d AND `id` < %d", pi.TableName, pi.Interval.Start, pi.Interval.End)
		result, err = pool.Execute(sql)
		fmt.Printf("Result %v, err %v\n", result, err)
		if err == nil {
			return result, nil
		} else {
			// FIXME: If the error looks like a schema issue, re-fetch the CREATE TABLE and parse the schema again
			// before we retry. (We also need some way to notify the sink that this has happened, so this might
			// need to be done at the runWorker level.)

			// 2**8 is about four and a half minutes, which is the longest we'll wait between retries.
			exponent := retries
			if exponent > 8 {
				exponent = 8
			}
			retries++
			time.Sleep(time.Second * time.Duration(math.Pow(2, float64(exponent))))
		}
	}

	return nil, err
}
