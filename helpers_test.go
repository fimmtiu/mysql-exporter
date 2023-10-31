package main

import (
	"context"
	"errors"
	"os"
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
	Columns []string
	Rows [][]any
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
		true,
		[]string{"Error"},
		[][]any{{err}},
	})
}

func (fake *FakeMysqlClient) Execute(query string, args ...interface{}) (IMysqlResult, error) {
	if len(fake.Responses) == 0 {
		return nil, errors.New("No fake MySQL responses left!")
	}
	response := fake.Responses[0]
	fake.Responses = fake.Responses[1:]

	if response.Error {
		return nil, errors.New(response.Rows[0][0].(string))
	} else {
		return &FakeMysqlResponse{false, response.Columns, response.Rows}, nil
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
