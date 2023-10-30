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
func SnapshotFakeConn(binlogPos int, gtidMax int, purgedGtids bool) *MysqlConnection {
	var isSubset int64 = 1
	if purgedGtids {
		isSubset = 0
	}
	return NewFakeMysqlConnection(
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
	state := SnapshotState{}

	// Happy path, doesn't need snapshotting
	conn := SnapshotFakeConn(31337, 35000, false)
	WithStateStorage(map[string]string{
		"last_committed_position": "4294998600",
		"last_committed_gtid_set": "3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30000",
	}, func() {
		assert.False(t, state.NeedsSnapshot(conn))
	})

	// Case where the server has purged GTIDs we need (purgedGtids == true)
	conn = SnapshotFakeConn(31337, 20000, true)
	WithStateStorage(map[string]string{
		"last_committed_position": "4294998600",
		"last_committed_gtid_set": "3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30000",
	}, func() {
		assert.True(t, state.NeedsSnapshot(conn))
	})

	// Case where the binlog position has reset (currentPosition < lastCommittedPosition)
	conn = SnapshotFakeConn(31337, 35000, false)
	WithStateStorage(map[string]string{
		"last_committed_position": "4700000000",
		"last_committed_gtid_set": "3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30000",
	}, func() {
		assert.True(t, state.NeedsSnapshot(conn))
	})
}

// Tests snapshotting a database of six tables, each with 10,005 records, by eighteen concurrent goroutines.
func TestSnapshotStateConcurrent(t *testing.T) {
	WithConfig("SNAPSHOT_CHUNK_SIZE", "100", func() {
		tableNames := []string{"foo", "bar", "baz", "quux", "honk", "bonk"}
		conn := SnapshotFakeConn(31337, 35000, false)
		state := NewSnapshotState(conn, tableNames)
		for _, tableName := range tableNames {
			assert.Equal(t, IntervalList{}, state.CompletedIntervals[tableName])
			assert.Equal(t, IntervalList{}, state.BusyIntervals[tableName])
			assert.Equal(t, IntervalList{Interval{0, 100}}, state.PendingIntervals[tableName])
		}
		assert.Empty(t, state.UpperBounds)

		// Three workers filling in each table.
		fmt.Printf("Before starting workers: %s\n", state.String())
		for i := 0; i < len(tableNames) * 3; i++ {
			go func(tableName string) {
				count := 0
				for {
					interval, ok := state.GetNextPendingInterval(tableName)
					if !ok {
						break
					}
					if interval.Start >= 10_000 {
						state.SetTableUpperBound(tableName, 10_004)
					}
					state.MarkIntervalDone(tableName, interval)
					count++
				}
			}(tableNames[i % len(tableNames)])
		}

		for !state.Done() {
			// busy-wait for everything to finish
		}

		for _, tableName := range tableNames {
			assert.Equal(t, IntervalList{Interval{0, 10_005}}, state.CompletedIntervals[tableName])
			assert.Equal(t, 10_005, state.UpperBounds[tableName])
		}
		assert.Empty(t, state.BusyIntervals)
		assert.Empty(t, state.PendingIntervals)
	})
}

// func TestSnapshotStatePicksUpFromLastStop(t *testing.T) {
// 	conn := SnapshotFakeConn(31337, 35000, false)
// 	state := NewSnapshotState(conn, tableNames)

// }

// Cases to test:
//
// - No snapshot state exists
//   - Can run concurrently and always end up with the correct state at the end
// - Snapshot state exists
//   - Picks up from where it left off without doing any repeated work




// func TestStateStorageLastCommittedGtid(t *testing.T) {
// 	storage := NewStateStorage()

// 	gtid, err := storage.GetLastCommittedGtid()
// 	assert.NoError(t, err)
// 	assert.Equal(t, uint64(0), gtid)

// 	err = storage.SetLastCommittedGtid(31337)
// 	assert.NoError(t, err)
// 	gtid, err = storage.GetLastCommittedGtid()
// 	assert.NoError(t, err)
// 	assert.Equal(t, uint64(31337), gtid)

// 	err = storage.ClearLastCommittedGtid()
// 	assert.NoError(t, err)
// 	gtid, err = storage.GetLastCommittedGtid()
// 	assert.NoError(t, err)
// 	assert.Equal(t, uint64(0), gtid)

// 	err = storage.SetLastCommittedGtid(31337)
// 	assert.NoError(t, err)
// 	err = storage.ClearAllState()
// 	assert.NoError(t, err)

// 	gtid, err = storage.GetLastCommittedGtid()
// 	assert.NoError(t, err)
// 	assert.Equal(t, uint64(0), gtid)
// }

// func TestStateStorageTableSnapshotState(t *testing.T) {
// 	WithConfig("SNAPSHOT_CHUNK_SIZE", "100", func() {
// 		storage := NewStateStorage()

// 		list, done, err := storage.GetTableSnapshotState("foo")
// 		assert.NoError(t, err)
// 		assert.False(t, done)
// 		assert.Equal(t, IntervalList{}, list)

// 		err = storage.SetTableSnapshotState("foo", IntervalList{Interval{0, 100}})
// 		assert.NoError(t, err)

// 		list, done, err = storage.GetTableSnapshotState("foo")
// 		assert.NoError(t, err)
// 		assert.False(t, done)
// 		assert.Equal(t, IntervalList{Interval{0, 100}}, list)

// 		list, done, err = storage.GetTableSnapshotState("bar")
// 		assert.NoError(t, err)
// 		assert.False(t, done)
// 		assert.Equal(t, IntervalList{}, list)

// 		err = storage.SetTableSnapshotState("bar", IntervalList{Interval{900, 1000}})
// 		assert.NoError(t, err)

// 		err = storage.SetTableSnapshotState("foo", IntervalList{Interval{0, 100},Interval{300, 400}})
// 		assert.NoError(t, err)

// 		list, done, err = storage.GetTableSnapshotState("foo")
// 		assert.NoError(t, err)
// 		assert.False(t, done)
// 		assert.Equal(t, IntervalList{Interval{0, 100}, Interval{300, 400}}, list)

// 		err = storage.MarkTableSnapshotDone("foo")
// 		assert.NoError(t, err)

// 		list, done, err = storage.GetTableSnapshotState("foo")
// 		assert.NoError(t, err)
// 		assert.True(t, done)
// 		assert.Nil(t, list)

// 		err = storage.ClearTableSnapshotState("foo")
// 		assert.NoError(t, err)

// 		list, done, err = storage.GetTableSnapshotState("foo")
// 		assert.NoError(t, err)
// 		assert.False(t, done)
// 		assert.Equal(t, IntervalList{}, list)

// 		list, done, err = storage.GetTableSnapshotState("bar")
// 		assert.NoError(t, err)
// 		assert.False(t, done)
// 		assert.Equal(t, IntervalList{Interval{900, 1000}}, list)

// 		err = storage.ClearAllState()
// 		assert.NoError(t, err)

// 		list, done, err = storage.GetTableSnapshotState("bar")
// 		assert.NoError(t, err)
// 		assert.False(t, done)
// 		assert.Equal(t, IntervalList{}, list)
// 	})
// }


