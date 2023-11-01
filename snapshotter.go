package main

import (
	"fmt"
	"math"
	"time"
)

const MAX_RETRIES = 10   // Picked this number out of the air. Let's revisit this later.

type Snapshotter struct {
	State *SnapshotState
	Workers *AsyncController
	PendingIntervalsChan chan PendingInterval
	CompletedIntervalsChan chan PendingInterval
	ExitChan chan struct{}
}

func NewSnapshotter() *Snapshotter {
	tables := getTableList()

	return &Snapshotter{
		NewSnapshotState(tables),
		NewAsyncController(),
		make(chan PendingInterval),
		make(chan PendingInterval),
		make(chan struct{}),
	}
}

func (s *Snapshotter) Run() {
	for i := 0; i < int(config.SnapshotWorkers); i++ {
		s.Workers.Go(s.runWorker)
	}
	logger.Printf("Started %d snapshot workers.", config.SnapshotWorkers)

	nextInterval, ok := s.State.GetNextPendingInterval()
	if !ok {
		panic(fmt.Errorf("No pending intervals at the start of the snapshot?"))
	}

	for {
		select {
		case s.PendingIntervalsChan <- nextInterval:
			nextInterval, ok = s.State.GetNextPendingInterval()
			if !ok {
				close(s.PendingIntervalsChan)
			}
		case completedInterval := <- s.CompletedIntervalsChan:
			err := s.State.MarkIntervalDone(completedInterval)
			if err != nil {
				panic(err)
			}
		case <-s.ExitChan:
			logger.Printf("Signalling all workers to exit.")
			s.Workers.Exit(nil)
			s.Workers.Wait()
			return
		case <-s.Workers.DoneSignal():
			logger.Printf("The snapshot is complete.")
			s.Workers.Wait()
			return
		}
	}
}

func (s *Snapshotter) Exit() {
	s.ExitChan <- struct{}{}
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
			for row := 0; row < result.RowNumber(); row++ {

			}

			s.CompletedIntervalsChan <- pi

		case <-s.Workers.ExitSignal():
			return nil
		}
	}
}

func getTableList() []string {
	tables, err := ListTables()
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(tables); i++ {
		if StringInList(tables[i], config.ExcludeTables) {
			tables = DeleteFromSlice(tables, i)
			i--
		}
	}
	return tables
}

func getRowChunk(pi PendingInterval) (IMysqlResult, error) {
	retries := 0
	var result IMysqlResult
	var err error

	for retries < MAX_RETRIES {
		sql := fmt.Sprintf("SELECT * FROM `%s` WHERE `id` >= %d AND `id` < %d", pi.TableName, pi.Interval.Start, pi.Interval.End)
		result, err = pool.Execute(sql)
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
