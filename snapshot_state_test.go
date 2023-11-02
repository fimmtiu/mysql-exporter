package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		workerGroup := NewWorkerGroup()
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
		state := NewSnapshotState(tableNames).(*RealSnapshotState)
		for _, tableName := range tableNames {
			tableState := state.Tables[tableName]
			assert.Equal(t, IntervalList{}, tableState.CompletedIntervals)
			assert.Equal(t, IntervalList{Interval{0, 100}}, tableState.BusyIntervals)
			assert.Equal(t, uint64(10_006), tableState.MaxId)
		}
		assert.Equal(t, 6, state.PendingIntervals.Len())
		assert.Equal(t, Interval{0, 100}, state.PendingIntervals.Front().Value.(PendingInterval).Interval)
		assert.Equal(t, Interval{0, 100}, state.PendingIntervals.Back().Value.(PendingInterval).Interval)

		inputChan := make(chan PendingInterval)
		outputChan := make(chan PendingInterval)

		// Three workers per table should ensure that they come into conflict.
		for i := 0; i < len(tableNames) * 3; i++ {
			workerGroup.Go(func() error {
				for {
					if inputChan == nil {
						break
					}
					interval, ok := <-inputChan
					if !ok {
						break
					}
					outputChan <- interval
				}
				return nil
			})
		}

		nextInterval, ok := state.GetNextPendingInterval()
		assert.True(t, ok)

		done:
		for {
			select {
			case inputChan <- nextInterval:
				nextInterval, ok = state.GetNextPendingInterval()
				if !ok {
					close(inputChan)
					inputChan = nil
				}
			case interval := <-outputChan:
				err := state.MarkIntervalDone(interval)
				assert.NoError(t, err)
			case <-workerGroup.DoneSignal():
				workerGroup.Wait()
				break done
			}
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
		state := NewSnapshotState(tableNames).(*RealSnapshotState)

		for i := 0; i < 10; i++ {
			interval, ok := state.GetNextPendingInterval()
			assert.True(t, ok)
			state.MarkIntervalDone(interval)
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
		state = NewSnapshotState(tableNames).(*RealSnapshotState)

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
