package main

import (
	"fmt"
	"slices"
	"strings"
	"sync"
)

// Half-open range, like [start, end). `End` is not in the interval.
type Interval struct {
	Start, End uint64
}

type IntervalList []Interval

func ParseInterval(s string) Interval {
	parts := strings.Split(s, "-")
	return Interval{uint64(MustParseInt(parts[0])), uint64(MustParseInt(parts[1]))}
}

func (i Interval) String() string {
	return fmt.Sprintf("%d-%d", i.Start, i.End)
}

func CompareIntervals(i, j Interval) int {
	if i.Start < j.Start {
		return -1
	} else if i.Start > j.Start {
		return 1
	} else {
		return 0
	}
}

func (i Interval) Includes(j Interval) bool {
	return i.Start <= j.Start && i.End >= j.End
}

func ParseIntervalList(s string) IntervalList {
	if s == "" {
		return IntervalList{}
	}
	intervals := strings.Split(s, ",")
	cl := make(IntervalList, len(intervals))
	for i, interval := range intervals {
		cl[i] = ParseInterval(interval)
	}
	return cl
}

func (list IntervalList) Includes(interval Interval) bool {
	for _, i := range list {
		if i.Includes(interval) {
			return true
		}
	}
	return false
}

func (list IntervalList) String() string {
	strs := make([]string, len(list))
	for i, interval := range list {
		strs[i] = interval.String()
	}
	return strings.Join(strs, ",")
}

// Add a chunk of completed work to the interval list. Returns a copy of 'list' with the interval added.
func (list IntervalList) Merge(interval Interval) IntervalList {
	list = append(list, interval)
	slices.SortFunc(list, CompareIntervals)

	// Coalesce any two adjacent intervals whose start and end are the same.
	for i := 0; i < len(list) - 1; i++ {
		if (list)[i].End == (list)[i + 1].Start {
			(list)[i].End = (list)[i + 1].End
			list = DeleteFromSlice(list, i + 1)
			i--
		}
	}
	return list
}

func (list IntervalList) HighestContiguous() uint64 {
	if len(list) == 0 {
		return 0
	}
	return list[0].End
}

func (list IntervalList) NextGap(maxSize uint64) Interval {
	switch len(list) {
	case 0:
		return Interval{0, uint64(maxSize)}
	case 1:
		return Interval{list[0].End, list[0].End + uint64(maxSize)}
	default:
		if maxSize > list[1].Start - list[0].End {
			maxSize = list[1].Start - list[0].End
		}
		return Interval{list[0].End, list[0].End + uint64(maxSize)}
	}
}

type SnapshotState struct {
	Lock sync.Mutex
	CompletedIntervals map[string]IntervalList
	BusyIntervals map[string]IntervalList
	PendingIntervals map[string]IntervalList
	UpperBounds map[string]uint64
}

func NewSnapshotState(conn *MysqlConnection, tableNames []string) *SnapshotState {
	var err error
	state := SnapshotState{
		sync.Mutex{},
		make(map[string]IntervalList, len(tableNames)),
		make(map[string]IntervalList, len(tableNames)),
		make(map[string]IntervalList, len(tableNames)),
		make(map[string]uint64, len(tableNames)),
	}
	needsSnapshot := state.NeedsSnapshot(conn)

	for _, tableName := range tableNames {
		progress := ""
		if needsSnapshot {
			err := stateStorage.Delete("table_snapshot_progress/" + tableName)
			if err != nil {
				panic(err)
			}
		} else {
			progress, err = stateStorage.Get("table_snapshot_progress/" + tableName)
			if err != nil {
				panic(err)
			}
		}

		if progress != "done" {
			state.CompletedIntervals[tableName] = ParseIntervalList(progress)
			state.BusyIntervals[tableName] = ParseIntervalList(progress)
			// We want to start with (number of gaps in the completed list) + 1 chunks.
			chunksToAdd := 1
			if len(state.CompletedIntervals[tableName]) > 0 {
				chunksToAdd = len(state.CompletedIntervals[tableName]) + 1
			}
			for i := 0; i < chunksToAdd; i++ {
				state.addNextPendingInterval(tableName)
			}
		}
	}

	fmt.Printf("New state: %s\n", state.String())
	return &state
}

func (state *SnapshotState) String() string {
	state.Lock.Lock()
	defer state.Lock.Unlock()

	return fmt.Sprintf("SnapshotState{\n  CompletedIntervals: %v,\n  BusyIntervals: %v,\n  PendingIntervals: %v,\n  UpperBounds: %v\n}", state.CompletedIntervals, state.BusyIntervals, state.PendingIntervals, state.UpperBounds)
}

// Returns `false` if there's no more work to do on this table.
func (state *SnapshotState) GetNextPendingInterval(tableName string) (Interval, bool) {
	state.Lock.Lock()
	defer state.Lock.Unlock()

	pending := state.PendingIntervals[tableName]
	if len(pending) == 0 {
		return Interval{}, false
	}

	interval := pending[len(pending) - 1]
	state.PendingIntervals[tableName] = pending[:len(pending) - 1]
	state.addNextPendingInterval(tableName)
	fmt.Printf("GetNextPendingInterval(%s): %v\n", tableName, interval)
	return interval, true
}

// Mark a chunk of work as done. If this is the last chunk of work for this table,
// mark the entire table as done.
func (state *SnapshotState) MarkIntervalDone(tableName string, interval Interval) error {
	state.Lock.Lock()
	defer state.Lock.Unlock()
	fmt.Printf("MarkIntervalDone(%s): %v\n", tableName, interval)

	completed := state.CompletedIntervals[tableName]
	if completed.Includes(interval) {
		panic(fmt.Errorf("Interval %v already completed for table %s (%v)", interval, tableName, completed))
	}
	state.CompletedIntervals[tableName] = completed.Merge(interval)
	upperBound, ok := state.UpperBounds[tableName]
	if ok && completed.HighestContiguous() == upperBound {
		return state.markTableDone(tableName)
	}
	return stateStorage.Set("table_snapshot_progress/" + tableName, completed.String())
}

// Once we know what the highest ID in the table is, we set it as the upper bound
// beyond which chunks will not be generated.
func (state *SnapshotState) SetTableUpperBound(tableName string, upperBound uint64) {
	state.Lock.Lock()
	defer state.Lock.Unlock()

	if currentBound, ok := state.UpperBounds[tableName]; !ok || currentBound > upperBound {
		state.UpperBounds[tableName] = upperBound
	}
}

// Returns true if all tables have been fully snapshotted.
func (state *SnapshotState) Done() bool {
	state.Lock.Lock()
	defer state.Lock.Unlock()

	return len(state.BusyIntervals) == 0
}

// If there's still work to do on this table (any gaps in the list of completed
// chunks, or chunks between the highest completed chunk and the upper bound),
// add a new chunk to the queue.
//
// Assumes that the lock is already held by the calling function.
func (state *SnapshotState) addNextPendingInterval(tableName string) {
	busy, ok := state.BusyIntervals[tableName]
	if !ok {   // the table's been removed by markTableDone(), so there's nothing to do
		return
	}
	pending := state.PendingIntervals[tableName]
	upperBound, haveUpperBound := state.UpperBounds[tableName]
	gap := busy.NextGap(config.SnapshotChunkSize)
	if haveUpperBound {
		fmt.Printf("table %s before: busy %v, pending %v, upperBound %d, have %v, gap %v\n", tableName, busy, pending, upperBound, haveUpperBound, gap)
		if gap.Start <= upperBound {
			gap.End = upperBound + 1
			state.BusyIntervals[tableName] = busy.Merge(gap)
			state.PendingIntervals[tableName] = pending.Merge(gap)
		}
		fmt.Printf("table %s after: busy %v, pending %v, upperBound %d, have %v, gap %v\n", tableName, busy, pending, upperBound, haveUpperBound, gap)
	} else {
		state.BusyIntervals[tableName] = busy.Merge(gap)
		state.PendingIntervals[tableName] = pending.Merge(gap)
	}
}

// Mark a table as done. This means that the entire table has been snapshotted and
// there are no more chunks to process.
//
// Assumes that the lock is already held by the calling function.
func (state *SnapshotState) markTableDone(tableName string) error {
	delete(state.BusyIntervals, tableName)
	delete(state.PendingIntervals, tableName)
	return stateStorage.Set("table_snapshot_progress/" + tableName, "done")
}

// True if we're out of sync with the replica and should start a new snapshot of
// all tables from scratch.
func (state *SnapshotState) NeedsSnapshot(conn *MysqlConnection) bool {
	strpos, err := stateStorage.Get("last_committed_position")
	if err != nil {
		panic(fmt.Errorf("Can't read last_committed_position from state storage: %s", err))
	}
	position := 0
	if len(strpos) > 0 {
		position = MustParseInt(strpos)
	}
	gtids, err := stateStorage.Get("last_committed_gtid_set")
	if err != nil {
		panic(fmt.Errorf("Can't read last_committed_gtid_set from state storage: %s", err))
	}

	currentPosition, currentGtids, err := conn.GetBinlogPosition()
	if err != nil {
		panic(err)
	}
	purgedGtidsExist, err := conn.DoPurgedGtidsExist(gtids, currentGtids)
	if err != nil {
		panic(err)
	}

	// If the current binlog position is less than the last committed position, it probably means that
	// the MySQL server was rebuilt from scratch after some sort of catastrophe.
	return currentPosition < uint64(position) || purgedGtidsExist
}
