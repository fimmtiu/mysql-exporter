package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseBinlogPosition(t *testing.T) {
	assert.Equal(t, uint64(0x10000007a69), ParseBinlogPosition("honk-bin-log.00001", int64(31337)))
	assert.Equal(t, uint64(0x57000148510e), ParseBinlogPosition("replica02-105169137-bin-log.000087", int64(21516558)))
}

func TestGetBinlogPosition(t *testing.T) {
	SetFakeResponses(
		FakeMysqlResponse{false, []string{}, [][]any{}}, // FLUSH TABLES
		FakeMysqlResponse{  // SHOW MASTER STATUS
			false,
			[]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"},
			[][]any{{"honk-bin-log.00001", int64(31337), "", "", "3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30243"}},
		},
		FakeMysqlResponse{false, []string{}, [][]any{}}, // UNLOCK TABLES
	)
	position, gtidset, err := GetBinlogPosition()

	assert.NoError(t, err)
	assert.Equal(t, uint64(0x10000007a69), position)
	assert.Equal(t, "3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30243", gtidset)
}

func TestDoPurgedGtidsExist(t *testing.T) {
	currentGtids := "3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30243"
	SetFakeResponses(
		FakeMysqlResponse{  // SELECT GTID_SUBSET
			false,
			[]string{"GTID_SUBSET(stuff)"},
			[][]any{{int64(1)}},
		},
	)
	result, err := DoPurgedGtidsExist("3a1b9647-46ad-11ee-8a65-0242c0a89007:1-30000", currentGtids)
	assert.NoError(t, err)
	assert.False(t, result)

	SetFakeResponses(
		FakeMysqlResponse{  // SELECT GTID_SUBSET
			false,
			[]string{"GTID_SUBSET(stuff)"},
			[][]any{{int64(0)}},
		},
	)
	result, err = DoPurgedGtidsExist("3a1b9647-46ad-11ee-8a65-0242c0a89007:1-40000", currentGtids)
	assert.NoError(t, err)
	assert.True(t, result)
}

func TestListTables(t *testing.T) {
	SetFakeResponses(
		FakeMysqlResponse{  // SHOW TABLES
			false,
			[]string{"Tables_in_honk"},
			[][]any{{"honk"}, {"bonk"}},
		},
	)
	tables, err := ListTables()
	assert.NoError(t, err)
	assert.Equal(t, []string{"honk", "bonk"}, tables)
}

func TestListTablesExcludesTables(t *testing.T) {
	SetFakeResponses(
		FakeMysqlResponse{  // SHOW TABLES
			false,
			[]string{"Tables_in_honk"},
			[][]any{{"foo"}, {"honk"}, {"bonk"}},
		},
	)

	WithConfig("EXCLUDE_TABLES", "honk", func() {
		tables, err := ListTables()
		assert.NoError(t, err)
		assert.Equal(t, []string{"foo", "bonk"}, tables)
	})
}


func TestGetTableSchema(t *testing.T) {
	SetFakeResponses(
		FakeMysqlResponse{  // SHOW CREATE TABLE
			false,
			[]string{"Table", "Create Table"},
			[][]any{{"email_addresses", MustReadFile("test_data/email_addresses_schema.sql")}},
		},
	)
	schema, err := GetTableSchema("email_addresses")
	assert.NoError(t, err)
	assert.Equal(t, "email_addresses", schema.Name)
	assert.Equal(t, "id", schema.Columns[0].Name)
	assert.Equal(t, "account_id", schema.Columns[8].Name)
}
