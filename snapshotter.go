package main

import "fmt"

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
		case interval, ok := <-s.PendingIntervalsChan:
			if !ok {
				return nil
			}
			// FIXME: get mysql rows
			// FIXME: write data to sink
			s.CompletedIntervalsChan <- interval
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
