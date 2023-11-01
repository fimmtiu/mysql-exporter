package main

import (
	"container/list"
	"fmt"
	"slices"
	"strings"
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
		if list[i].End == list[i + 1].Start {
			list[i].End = list[i + 1].End
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

type SnapshotTableState struct {
	TableName string
	CompletedIntervals IntervalList
	BusyIntervals IntervalList
	MaxId uint64
}

type PendingInterval struct {
	TableName string
	Interval Interval
}

type SnapshotState struct {
	Tables map[string]*SnapshotTableState
	PendingIntervals *list.List
}

func NewSnapshotState(tableNames []string) *SnapshotState {
	var err error
	state := SnapshotState{
		make(map[string]*SnapshotTableState, len(tableNames)),
		list.New(),
	}
	needsSnapshot := needsSnapshot()

	// Populate the list of tables which haven't yet been completely snapshotted.
	// (If needsSnapshot is true, that's all of them.)
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
			tableState := SnapshotTableState{
				tableName,
				ParseIntervalList(progress),
				ParseIntervalList(progress),
				getHighestTableId(tableName),
			}
			state.Tables[tableName] = &tableState
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
func (state *SnapshotState) GetNextPendingInterval() (PendingInterval, bool) {
	if state.PendingIntervals.Front() == nil {
		return PendingInterval{}, false
	}

	nextInterval := state.PendingIntervals.Remove(state.PendingIntervals.Front()).(PendingInterval)
	tableState := state.Tables[nextInterval.TableName]
	state.addNextPendingInterval(tableState)
	fmt.Printf("GetNextPendingInterval(): %v\n", nextInterval)
	return nextInterval, true
}

// Mark a chunk of work as done. If this is the last chunk of work for this table,
// mark the entire table as done.
func (state *SnapshotState) MarkIntervalDone(pendingInterval PendingInterval) error {
	tableState := state.Tables[pendingInterval.TableName]
	if tableState.CompletedIntervals.Includes(pendingInterval.Interval) {
		panic(fmt.Errorf("Interval %v already completed for table %s (%v)", pendingInterval.Interval, tableState.TableName, tableState.CompletedIntervals))
	}
	tableState.CompletedIntervals = tableState.CompletedIntervals.Merge(pendingInterval.Interval)
	fmt.Printf("MarkIntervalDone(): %v (completed %v)\n", pendingInterval, tableState.CompletedIntervals)
	if tableState.CompletedIntervals.HighestContiguous() > tableState.MaxId {
		return state.markTableDone(tableState.TableName)
	}
	return stateStorage.Set("table_snapshot_progress/" + tableState.TableName, tableState.CompletedIntervals.String())
}

// Returns true if all tables have been fully snapshotted.
func (state *SnapshotState) Done() bool {
	return len(state.Tables) == 0
}

// If there's still work to do on this table (any gaps in the list of completed
// chunks, or chunks between the highest completed chunk and the upper bound),
// add a new chunk to the work queue.
func (state *SnapshotState) addNextPendingInterval(table *SnapshotTableState) {
	gap := table.BusyIntervals.NextGap(config.SnapshotChunkSize)
	if gap.Start <= table.MaxId {
		if gap.End > table.MaxId {
			gap.End = table.MaxId + 1
		}
		table.BusyIntervals = table.BusyIntervals.Merge(gap)
		state.PendingIntervals.PushBack(PendingInterval{table.TableName, gap})
	}
}

// Mark a table as done. This means that the entire table has been snapshotted and
// there are no more chunks to process.
func (state *SnapshotState) markTableDone(tableName string) error {
	delete(state.Tables, tableName)
	return stateStorage.Set("table_snapshot_progress/" + tableName, "done")
}

// True if we're out of sync with the replica and should start a new snapshot of
// all tables from scratch.
func needsSnapshot() bool {
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
