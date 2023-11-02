package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotterDoesNoWork(t *testing.T) {
	state := NewFakeSnapshotState([]string{}, 0)
	snapshotter := NewCustomSnapshotter(state)

	assert.True(t, state.Done())
	assert.True(t, snapshotter.Run())
}

func TestSnapshotterExit(t *testing.T) {
	SetFakeResponses(
		FakeMysqlResponse{false, math.MaxInt, []string{"foo"}, [][]any{{"bar"}}},
	)
	state := NewFakeSnapshotState([]string{"foo"}, 10)
	snapshotter := NewCustomSnapshotter(state)

	assert.False(t, state.Done())
	snapshotter.Exit()
	assert.False(t, snapshotter.Run())
}

func TestSnapshotterCompletes(t *testing.T) {
	WithConfig("SNAPSHOT_CHUNK_SIZE", "100", func() {
		state := NewFakeSnapshotState([]string{"foo", "bar", "baz", "quux"}, 10000)
		snapshotter := NewCustomSnapshotter(state)
		assert.True(t, snapshotter.Run())
	})
}
