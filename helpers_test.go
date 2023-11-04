package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// This file contains test-specific helper code and is only compiled in test mode.

func init() {
	stateStorage.ClearAll()

	pool = &FakeMysqlPool{FakeMysqlClient{true, []FakeMysqlResponse{}}}
}

func WithConfig(key string, val string, fn func()) {
	os.Setenv(key, val)
	oldConfig := config
	config = NewConfig()
	defer os.Unsetenv(key)
	defer func() { config = oldConfig }()

	fn()
}

func WithStateStorage(contents map[string]string, fn func()) {
	oldStateStorage := stateStorage
	stateStorage = NewStateStorageMemory()
	defer func() { stateStorage = oldStateStorage }()
	for key, val := range contents {
		stateStorage.Set(key, val)
	}

	fn()
}

type FakeMysqlResponse struct {
	Error bool
	Repeat int
	Columns []string
	Rows [][]any
}

func (response *FakeMysqlResponse) GetValue(row, column int) (any, error) {
	return response.Rows[row][column], nil
}

func (response *FakeMysqlResponse) GetString(row, column int) (string, error) {
	return response.Rows[row][column].(string), nil
}

func (response *FakeMysqlResponse) GetInt(row, column int) (int64, error) {
	return response.Rows[row][column].(int64), nil
}

func (response *FakeMysqlResponse) RowNumber() int {
	return len(response.Rows)
}

type FakeMysqlClient struct {
	Connected bool
	Responses []FakeMysqlResponse
}

func (fake *FakeMysqlClient) Close() error {
	fake.Connected = false
	return nil
}

func (fake *FakeMysqlClient) AddResponse(response FakeMysqlResponse) {
	fake.Responses = append(fake.Responses, response)
}

func (fake *FakeMysqlClient) AddErrorResponse(err string) {
	fake.Responses = append(fake.Responses, FakeMysqlResponse{
		true, 0,
		[]string{"Error"},
		[][]any{{err}},
	})
}

func (fake *FakeMysqlClient) Execute(query string, args ...interface{}) (IMysqlResult, error) {
	if len(fake.Responses) == 0 {
		return nil, errors.New("No fake MySQL responses left!")
	}
	fake.Responses[0].Repeat--
	response := fake.Responses[0]
	if response.Repeat <= 0 {
		fake.Responses = fake.Responses[1:]
	}

	if response.Error {
		return nil, errors.New(response.Rows[0][0].(string))
	} else {
		return &response, nil
	}
}

func SetFakeResponses(responses... FakeMysqlResponse) {
	pool.(*FakeMysqlPool).Client.Responses = responses
}

func AddFakeResponses(responses... FakeMysqlResponse) {
	pool.(*FakeMysqlPool).Client.Responses = append(pool.(*FakeMysqlPool).Client.Responses, responses...)
}

type FakeMysqlPool struct {
	Client FakeMysqlClient
}

func (pool *FakeMysqlPool) Execute(query string, args ...interface{}) (IMysqlResult, error) {
	return pool.Client.Execute(query, args...)
}

func (pool *FakeMysqlPool) GetConn(ctx context.Context) (IMysqlClient, error) {
	return &pool.Client, nil
}

func (pool *FakeMysqlPool) PutConn(conn IMysqlClient) {
	// No-op.
}

func TestPoolExecute(t *testing.T) {
	SetFakeResponses(
		FakeMysqlResponse{false, 3, []string{"foo"}, [][]any{{"bar"}}},
	)

	for i := 0; i < 3; i++ {
		result, err := pool.Execute("SELECT foo")
		assert.NoError(t, err)
		value, err := result.GetString(0, 0)
		assert.Equal(t, "bar", value)
	}

	result, err := pool.Execute("SELECT foo")
	assert.Nil(t, result)
	assert.Error(t, err)
}

type FakeSnapshotStateTable struct {
	Schema *TableSchema
	PendingIntervals IntervalList
	CompletedIntervals IntervalList
}

type FakeSnapshotState struct {
	FinalInterval Interval
	Tables []*FakeSnapshotStateTable
}

func NewFakeSnapshotState(tableNames []string, rowsPerTable int) SnapshotState {
	numberOfChunks := int(math.Ceil(float64(rowsPerTable) / float64(config.SnapshotChunkSize)))
	state := FakeSnapshotState{FinalInterval: Interval{0, uint64(numberOfChunks) * config.SnapshotChunkSize}}
	for _, tableName := range tableNames {
		schema := &TableSchema{tableName, []Column{{"id", "bigint", 20, 0, false, false}}}
		table := FakeSnapshotStateTable{schema, IntervalList{}, IntervalList{}}
		for i := 0; i < numberOfChunks; i++ {
			table.PendingIntervals = append(table.PendingIntervals, Interval{uint64(i) * config.SnapshotChunkSize, uint64(i + 1) * config.SnapshotChunkSize})
		}
		state.Tables = append(state.Tables, &table)
	}
	return &state
}

func (state *FakeSnapshotState) GetNextPendingInterval() (PendingInterval, bool) {
	table := state.Tables[rand.Intn(len(state.Tables))]
	if len(table.PendingIntervals) == 0 {
		return PendingInterval{}, false
	} else {
		interval := table.PendingIntervals[0]
		table.PendingIntervals = table.PendingIntervals[1:]
		return PendingInterval{table.Schema, interval}, true
	}
}

func (state *FakeSnapshotState) MarkIntervalDone(pendingInterval PendingInterval) error {
	for i, table := range state.Tables {
		if table.Schema.Name == pendingInterval.Schema.Name {
			table.PendingIntervals = table.PendingIntervals.Subtract(pendingInterval.Interval)
			table.CompletedIntervals = table.CompletedIntervals.Merge(pendingInterval.Interval)
			if len(table.CompletedIntervals) == 1 && table.CompletedIntervals[0] == state.FinalInterval {
				state.Tables = DeleteFromSlice(state.Tables, i)
			}
			return nil
		}
	}
	panic(fmt.Errorf("No such table: %s", pendingInterval.Schema.Name))
}

func (state *FakeSnapshotState) Done() bool {
	return len(state.Tables) == 0
}
