package main

type Snapshotter struct {
	State *SnapshotState
	Workers *AsyncController
	ExitChan chan struct{}
}

func NewSnapshotter() *Snapshotter {
	tables := getTableList()

	return &Snapshotter{
		State: NewSnapshotState(tables),
		Workers: NewAsyncController(),
		ExitChan: make(chan struct{}),
	}
}

func (s *Snapshotter) Run() {
	for i := 0; i < int(config.SnapshotWorkers); i++ {
		s.Workers.Go(s.runWorker)
	}
	logger.Printf("Started %d snapshot workers.", config.SnapshotWorkers)

	select {
	case <-s.ExitChan:
		logger.Printf("Signalling all workers to exit.")
		s.Workers.Exit(nil)
	case <-s.Workers.DoneSignal():
		logger.Printf("The snapshot is complete.")
	}
	s.Workers.Wait()
}

func (s *Snapshotter) Exit() {
	s.ExitChan <- struct{}{}
}

func (s *Snapshotter) runWorker() error {
	for {
		select {

		case <-s.Workers.ExitSignal():
			break
		}
	}
}

func getTableList() []string {
	tables, err := ListTables()
	if err != nil {
		panic(err)
	}

	for i := range tables {
		if StringInList(tables[i], config.ExcludeTables) {
			tables = DeleteFromSlice(tables, i)
		}
	}
	return tables
}
