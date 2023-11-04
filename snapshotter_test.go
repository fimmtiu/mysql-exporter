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

func TestSnapshotterCompletesWithSink(t *testing.T) {
	sinks = []Sink{NewCsvSink()}
	defer func() { sinks = nil }()

	tables := []string{"foo", "bar", "baz", "quux"}
	for _, table := range tables {
		sinks[0].Open(&TableSchema{table, []Column{{"id", "bigint", 20, 0, false, false}}})
	}

	WithConfig("SNAPSHOT_CHUNK_SIZE", "100", func() {
		state := NewFakeSnapshotState(tables, 10000)
		snapshotter := NewCustomSnapshotter(state)
		assert.True(t, snapshotter.Run())
	})
}
