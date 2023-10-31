package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseIntervalList(t *testing.T) {
	list := ParseIntervalList("")
	assert.Equal(t, IntervalList{}, list)

	list = ParseIntervalList("0-200,500-600,10010-31337")
	expected := IntervalList{
		Interval{0, 200},
		Interval{500, 600},
		Interval{10010, 31337},
	}
	assert.Equal(t, expected, list)
}

func TestIntervalListString(t *testing.T) {
	assert.Equal(t, "", IntervalList{}.String())

	list := IntervalList{
		Interval{0, 200},
		Interval{500, 600},
		Interval{10010, 31337},
	}
	assert.Equal(t, "0-200,500-600,10010-31337", list.String())
}

func TestIntervalListMerge(t *testing.T) {
	list := IntervalList{
		Interval{0, 200},
		Interval{500, 600},
	}
	list = list.Merge(Interval{200, 300})
	assert.Equal(t, "0-300,500-600", list.String())
	list = list.Merge(Interval{600, 700})
	assert.Equal(t, "0-300,500-700", list.String())
	list = list.Merge(Interval{300, 400})
	assert.Equal(t, "0-400,500-700", list.String())
	list = list.Merge(Interval{400, 500})
	assert.Equal(t, "0-700", list.String())
	list = list.Merge(Interval{10_000, 10_100})
	assert.Equal(t, "0-700,10000-10100", list.String())
}

func TestIntervalListNextGap(t *testing.T) {
	s := IntervalList{}.NextGap(1000).String()
	assert.Equal(t, "0-1000", s)
	s = IntervalList{Interval{0, 2000}}.NextGap(1000).String()
	assert.Equal(t, "2000-3000", s)
	s = IntervalList{Interval{0, 2000}, Interval{4000, 5000}}.NextGap(1000).String()
	assert.Equal(t, "2000-3000", s)
	s = IntervalList{Interval{0, 2000}, Interval{4000, 5000}}.NextGap(5000).String()
	assert.Equal(t, "2000-4000", s)
}

// Helper to create a fake MySQL connection with the boilerplate responses needed by NewSnapshotState().
func SetFakeSnapshotResponses(binlogPos int, gtidMax int, purgedGtids bool) {
	var isSubset int64 = 1
	if purgedGtids {
		isSubset = 0
	}
	SetFakeResponses(
		FakeMysqlResponse{false, []string{}, [][]any{}}, // FLUSH TABLES
		FakeMysqlResponse{  // SHOW MASTER STATUS
			false,
			[]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"},
			[][]any{{"honk-bin-log.00001", int64(binlogPos), "", "", fmt.Sprintf("3a1b9647-46ad-11ee-8a65-0242c0a89007:1-%d", gtidMax)}},
		},
		FakeMysqlResponse{false, []string{}, [][]any{}}, // UNLOCK TABLES,
		FakeMysqlResponse{false, []string{"GTID_SUBSET(...)"}, [][]any{{isSubset}}}, // SELECT GTID_SUBSET
	)
}

func TestSnapshotStateNeedsSnapshot(t *testing.T) {
	// Happy path, doesn't need snapshotting
	SetFakeSnapshotResponses(31337, 35000, false)
	WithStateStorage(map[string]string{
		"last_committed_position": "1099511659000",
		"last_committed_gtid_set": "3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30000",
	}, func() {
		assert.False(t, needsSnapshot())
	})

	// Case where the server has purged GTIDs we need (purgedGtids == true)
	SetFakeSnapshotResponses(31337, 20000, true)
	WithStateStorage(map[string]string{
		"last_committed_position": "1099511659000",
		"last_committed_gtid_set": "3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30000",
	}, func() {
		assert.True(t, needsSnapshot())
	})

	// Case where the binlog position has reset (currentPosition < lastCommittedPosition)
	SetFakeSnapshotResponses(31337, 35000, false)
	WithStateStorage(map[string]string{
		"last_committed_position": "1099511659555",
		"last_committed_gtid_set": "3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30000",
	}, func() {
		assert.True(t, needsSnapshot())
	})
}

// Tests snapshotting a database of six tables, each with 10,005 records (ids 1–10006),
// by eighteen concurrent goroutines.
func TestSnapshotStateConcurrent(t *testing.T) {
	stateStorage.ClearAll()
	WithConfig("SNAPSHOT_CHUNK_SIZE", "100", func() {
		tableNames := []string{"foo", "bar", "baz", "quux", "honk", "bonk"}
		SetFakeSnapshotResponses(31337, 35000, false)
		AddFakeResponses(
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(10_006)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(10_006)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(10_006)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(10_006)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(10_006)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(10_006)}}},
		)
		state := NewSnapshotState(tableNames)
		for _, tableName := range tableNames {
			tableState := state.Tables[tableName]
			assert.Equal(t, IntervalList{}, tableState.CompletedIntervals)
			assert.Equal(t, IntervalList{Interval{0, 100}}, tableState.BusyIntervals)
			assert.Equal(t, uint64(10_006), tableState.MaxId)
		}
		assert.Equal(t, 6, state.PendingIntervals.Len())
		assert.Equal(t, Interval{0, 100}, state.PendingIntervals.Front().Value.(PendingInterval).Interval)
		assert.Equal(t, Interval{0, 100}, state.PendingIntervals.Back().Value.(PendingInterval).Interval)

		// Three workers filling in each table.
		for i := 0; i < len(tableNames) * 3; i++ {
			go func() {
				for {
					interval, ok := state.GetNextPendingInterval()
					if !ok {
						break
					}
					state.MarkIntervalDone(interval)
				}
			}()
		}

		for !state.Done() {
			// busy-wait for everything to finish
		}

		assert.Empty(t, state.Tables)
		assert.Equal(t, 0, state.PendingIntervals.Len())
	})
}

func TestSnapshotStatePicksUpFromLastStop(t *testing.T) {
	stateStorage.ClearAll()
	WithConfig("SNAPSHOT_CHUNK_SIZE", "100", func() {
		tableNames := []string{"foo", "bar", "baz", "quux", "honk", "bonk"}
		SetFakeSnapshotResponses(31337, 35000, false)
		AddFakeResponses(
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
		)
		state := NewSnapshotState(tableNames)
		fmt.Printf("Pending intervals len %d\n", state.PendingIntervals.Len())
		for e := state.PendingIntervals.Front(); e != nil; e = e.Next() {
			fmt.Printf("Pending interval: %v\n", e.Value)
		}

		for i := 0; i < 10; i++ {
			interval, ok := state.GetNextPendingInterval()
			fmt.Printf("interval: %v\n", interval)
			assert.True(t, ok)
			state.MarkIntervalDone(interval)
		}

		for _, tableState := range state.Tables {
			fmt.Printf("Completed: %v\n", tableState.CompletedIntervals)
		}

		// Now that we've done 10 chunks of work, we're going to stop and start again.
		// The new state should pick up where the previous one left off, with 50 chunks
		// of work remaining.
		SetFakeSnapshotResponses(31337, 35000, false)
		AddFakeResponses(
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
			FakeMysqlResponse{false, []string{"MAX(id)"}, [][]any{{int64(999)}}},
		)
		state = NewSnapshotState(tableNames)

		count := 0
		for !state.Done() {
			count++
			interval, ok := state.GetNextPendingInterval()
			assert.True(t, ok)
			state.MarkIntervalDone(interval)
		}
		assert.Equal(t, 50, count)
	})
}
