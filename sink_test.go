package main

import (
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCsvSink(t *testing.T) {
	responseChan := make(chan error)
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

	bloom, _ := time.Parse("2006-01-02T15:04:05Z", "1904-06-16T11:34:56Z")
	phoenix, _ := time.Parse("2006-01-02T15:04:05Z", "2063-04-04T20:10:31Z")
	pele, _ := time.Parse("2006-01-02T15:04:05Z", "1996-01-22T23:45:06Z")
	ocean, _ := time.Parse("2006-01-02T15:04:05Z", "2021-10-29T06:05:22Z")
	dateRows := RowsEvent{responseChan, schemas[0], [][]any{{
		int32(1337), int32(9517), bloom, phoenix, pele, ocean,
	}}}
	expectedDates := "date_o,date_r,datetime_o,datetime_r,timestamp_o,timestamp_r\n" + `"1973-08-30","1996-01-22","1904-06-16 11:34:56","2063-04-04 20:10:31","1996-01-22 23:45:06","2021-10-29 06:05:22"` + "\n"

	numberRows := RowsEvent{responseChan, schemas[1], [][]any{{
		int8(-1), int8(-120), uint8(0), uint8(120), int16(500), int16(-500), uint16(30000), uint16(60000), int32(-8000000), int32(8000000), uint32(500), uint32(10000000), int32(-2000000000), int32(2000000000), uint32(3000000000), uint32(4000000000), int64(-900000000000000000), int64(900000000000000000), uint64(31337), uint64(1000000000000000000), float32(0.1), float32(313.37), float64(3.1337), float64(0.0),
		// FIXME add decimals
		nil, nil, nil, nil, nil, nil,
	}}}
	expectedNumbersRegexp := regexp.MustCompile("^tinyint_so,tinyint_sr,tinyint_uo,tinyint_ur,smallint_so,smallint_sr,smallint_uo,smallint_ur,mediumint_so,mediumint_sr,mediumint_uo,mediumint_ur,int_so,int_sr,int_uo,int_ur,bigint_so,bigint_sr,bigint_uo,bigint_ur,float_o,float_r,double_o,double_r,smalldecimal_o,smalldecimal_r,mediumdecimal_o,mediumdecimal_r,bigdecimal_o,bigdecimal_r\n" + `-1,-120,0,120,500,-500,30000,60000,-8000000,8000000,500,10000000,-2000000000,2000000000,3000000000,4000000000,-900000000000000000,900000000000000000,31337,1000000000000000000,0\.10+,313\.3\d+,3\.133\d+,0\.0+,,,,,,` + "\n$")

	stringRows := RowsEvent{responseChan, schemas[2], [][]any{{
		"woop", "bloop", `I like "pie"`, `wh"eeee`, "this has,a comma", "this,has two,commas", "honk", "bonk", "", "...", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
	}}}
	expectedStrings := "char_o,char_r,varchar_o,varchar_r,text_o,text_r,mediumtext_o,mediumtext_r,longtext_o,longtext_r,binary_o,binary_r,varbinary_o,varbinary_r,blob_o,blob_r,mediumblob_o,mediumblob_r,longblob_o,longblob_r\n" + `woop,bloop,"I like ""pie""","wh""eeee","this has,a comma","this,has two,commas",honk,bonk,,...,a,b,c,d,e,f,g,h,i,j` + "\n"

	sink.WriteRows(dateRows)
	assert.NoError(t, <- responseChan)
	sink.WriteRows(numberRows)
	assert.NoError(t, <- responseChan)
	sink.WriteRows(stringRows)
	assert.NoError(t, <- responseChan)

	assert.NoError(t, sink.Exit())

	assert.Equal(t, expectedDates, MustReadFile(fmt.Sprintf("/tmp/%d/all_date_types_1.csv", os.Getpid())))
	assert.Regexp(t, expectedNumbersRegexp, MustReadFile(fmt.Sprintf("/tmp/%d/all_number_types_1.csv", os.Getpid())))
	assert.Equal(t, expectedStrings, MustReadFile(fmt.Sprintf("/tmp/%d/all_string_types_1.csv", os.Getpid())))
}
