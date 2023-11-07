package main

import (
	"container/list"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type SnapshotTableState struct {
	Schema *TableSchema
	CompletedIntervals IntervalList
	BusyIntervals IntervalList
	MaxId uint64
}

type PendingInterval struct {
	Schema *TableSchema
	Interval Interval
}

type SnapshotState interface {
	GetNextPendingInterval() (PendingInterval, bool)
	MarkIntervalDone(pendingInterval PendingInterval) error
	Done() bool
}

// As opposed to the FakeSnapshotState that we use in some of the tests.
type RealSnapshotState struct {
	Tables map[string]*SnapshotTableState
	PendingIntervals *list.List
}

func NewSnapshotState(tables []*TableSchema) SnapshotState {
	var err error
	state := RealSnapshotState{
		make(map[string]*SnapshotTableState, len(tables)),
		list.New(),
	}
	needsSnapshot := needsSnapshot()

	// Populate the list of tables which haven't yet been completely snapshotted.
	// (If needsSnapshot is true, that's all of them.)
	for _, table := range tables {
		progress := ""
		if needsSnapshot {
			err := stateStorage.Delete("table_snapshot_progress/" + table.Name)
			if err != nil {
				panic(err)
			}
		} else {
			progress, err = stateStorage.Get("table_snapshot_progress/" + table.Name)
			if err != nil && err != redis.Nil {
				panic(err)
			}
		}

		if progress != "done" {
			tableState := SnapshotTableState{
				table,
				ParseIntervalList(progress),
				ParseIntervalList(progress),
				getHighestTableId(table.Name),
			}
			state.Tables[table.Name] = &tableState
		}
	}

	// Populate the PendingIntervals list with work that needs to be done for each incomplete table.
	for _, tableState := range state.Tables {
		// We want to start with (number of gaps in the completed list) + 1 chunks.
		chunksToAdd := 1
		if len(tableState.CompletedIntervals) > 0 {
			chunksToAdd = len(tableState.CompletedIntervals) + 1
		}
		for i := 0; i < chunksToAdd; i++ {
			state.addNextPendingInterval(tableState)
		}
	}

	return &state
}

// Returns `false` if there's no more work for any worker to do.
func (state *RealSnapshotState) GetNextPendingInterval() (PendingInterval, bool) {
	if state.PendingIntervals.Front() == nil {
		return PendingInterval{}, false
	}
	nextInterval := state.PendingIntervals.Remove(state.PendingIntervals.Front()).(PendingInterval)
	tableState := state.Tables[nextInterval.Schema.Name]
	state.addNextPendingInterval(tableState)
	return nextInterval, true
}

// Mark a chunk of work as done. If this is the last chunk of work for this table,
// mark the entire table as done.
func (state *RealSnapshotState) MarkIntervalDone(pi PendingInterval) error {
	logger.Printf("Started MarkIntervalDone for %s (%v)", pi.Schema.Name, pi.Interval)
	defer logger.Printf("Finished MarkIntervalDone for %s (%v)", pi.Schema.Name, pi.Interval)

	tableState := state.Tables[pi.Schema.Name]
	if tableState.CompletedIntervals.Includes(pi.Interval) {
		panic(fmt.Errorf("Interval %v already completed for table %s (%v)", pi.Interval, tableState.Schema.Name, tableState.CompletedIntervals))
	}
	tableState.CompletedIntervals = tableState.CompletedIntervals.Merge(pi.Interval)
	if tableState.CompletedIntervals.HighestContiguous() > tableState.MaxId {
		return state.markTableDone(tableState.Schema.Name)
	}
	return stateStorage.Set("table_snapshot_progress/" + tableState.Schema.Name, tableState.CompletedIntervals.String())
}

// Returns true if all tables have been fully snapshotted.
func (state *RealSnapshotState) Done() bool {
	return len(state.Tables) == 0
}

// If there's still work to do on this table (any gaps in the list of completed
// chunks, or chunks between the highest completed chunk and the upper bound),
// add a new chunk to the work queue.
func (state *RealSnapshotState) addNextPendingInterval(table *SnapshotTableState) {
	gap := table.BusyIntervals.NextGap(config.SnapshotChunkSize)
	if gap.Start <= table.MaxId {
		if gap.End > table.MaxId {
			gap.End = table.MaxId + 1
		}
		table.BusyIntervals = table.BusyIntervals.Merge(gap)
		state.PendingIntervals.PushBack(PendingInterval{table.Schema, gap})
	}
}

// Mark a table as done. This means that the entire table has been snapshotted and
// there are no more chunks to process.
func (state *RealSnapshotState) markTableDone(tableName string) error {
	logger.Printf("Snapshot of table '%s' is complete.", tableName)
	delete(state.Tables, tableName)
	return stateStorage.Set("table_snapshot_progress/" + tableName, "done")
}

// True if we're out of sync with the replica and should start a new snapshot of
// all tables from scratch.
func needsSnapshot() bool {
	strpos, err := stateStorage.Get("last_committed_position")
	if err != nil && err != redis.Nil {
		panic(fmt.Errorf("Can't read last_committed_position from state storage: %s", err))
	}
	position := 0
	if len(strpos) > 0 {
		position = MustParseInt(strpos)
	}
	gtids, err := stateStorage.Get("last_committed_gtid_set")
	if err != nil && err != redis.Nil {
		panic(fmt.Errorf("Can't read last_committed_gtid_set from state storage: %s", err))
	}

	currentPosition, currentGtids, err := GetBinlogPosition()
	if err != nil {
		panic(err)
	}
	purgedGtidsExist, err := DoPurgedGtidsExist(gtids, currentGtids)
	if err != nil {
		panic(err)
	}

	// If the current binlog position is less than the last committed position, it probably means that
	// the MySQL server was rebuilt from scratch after some sort of catastrophe.
	return currentPosition < uint64(position) || purgedGtidsExist
}

func getHighestTableId(tableName string) uint64 {
	result, err := pool.Execute("SELECT MAX(id) FROM `" + tableName + "`")
	if err != nil {
		panic(err)
	}
	signedMaxId, err := result.GetInt(0, 0)
	if err != nil {
		panic(err)
	}
	return uint64(signedMaxId)
}
