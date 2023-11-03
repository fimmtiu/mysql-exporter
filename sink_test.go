package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCsvSink(t *testing.T) {
	schemaFiles := []string{"all_date_types.sql", "all_number_types.sql", "all_string_types.sql"}
	schemas := make([]*TableSchema, len(schemaFiles))
	for i, schemaFile := range schemaFiles {
		schemas[i] = ParseSchema(MustReadFile("test_data/" + schemaFile))
	}

	assert.NoError(t, os.RemoveAll(fmt.Sprintf("/tmp/%d", os.Getpid())))
	sink := NewCsvSink()

	for _, schema := range schemas {
		assert.NoError(t, sink.Open(schema))
	}

	responseChan := make(chan error)
	bloom, _ := time.Parse("2006-01-02T15:04:05Z", "1904-06-16T11:34:56Z")
	phoenix, _ := time.Parse("2006-01-02T15:04:05Z", "2063-04-04T20:10:31Z")
	pele, _ := time.Parse("2006-01-02T15:04:05Z", "1996-01-22T23:45:06Z")
	ocean, _ := time.Parse("2006-01-02T15:04:05Z", "2021-10-29T06:05:22Z")
	dateRows := RowsEvent{responseChan, schemas[0], []any{
		int32(1337), int32(9517), bloom, phoenix, pele, ocean,
	}}
	expectedDates := "date_o,date_r,datetime_o,datetime_r,timestamp_o,timestamp_r\n" + `"1973-08-30","1996-01-22","1904-06-16 11:34:56","2063-04-04 20:10:31","1996-01-22 23:45:06","2021-10-29 06:05:22"` + "\n"
	sink.WriteRow(schemas[0], dateRows)
	assert.NoError(t, <- responseChan)


	// numberRows := ""
	// stringRows := ""


	assert.NoError(t, sink.Exit())

	assert.Equal(t, expectedDates, MustReadFile(fmt.Sprintf("/tmp/%d/all_date_types.csv", os.Getpid())))
}
