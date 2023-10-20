package main

import (
	"errors"
	"os"
)

// This file contains test-specific helper code and is only compiled in test mode.

func init() {
	stateStorage.ClearAll()
}

func WithConfig(key string, val string, fn func()) {
	os.Setenv(key, val)
	oldConfig := config
	config = NewConfig()
	defer os.Unsetenv(key)
	defer func() { config = oldConfig }()

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
	response := fake.Responses[0]
	fake.Responses = fake.Responses[1:]

	if response.Error {
		return nil, errors.New(response.Rows[0][0].(string))
	} else {
		return &FakeMysqlResponse{false, response.Columns, response.Rows}, nil
	}
}

func NewFakeMysqlConnection(responses... FakeMysqlResponse) *MysqlConnection {
	return &MysqlConnection{&FakeMysqlClient{true, responses}}
}

// func NewFakeMysqlResultset(schema *TableSchema, rows [][]any) *mysql.Resultset {
// 	resultset := mysql.NewResultset(len(schema.Columns))
// 	for i, column := range schema.Columns {
// 		resultset.Fields[i] = &mysql.Field{Type: column.MysqlLibraryType()}
// 		if !column.Signed {
// 			resultset.Fields[i].Flag |= mysql.UNSIGNED_FLAG
// 		}

// 		for _, row := range rows {
// 			value := mysql.FieldValue{Type: mysql.FieldValueTypeNull}
// 		}
// 	}
// }
