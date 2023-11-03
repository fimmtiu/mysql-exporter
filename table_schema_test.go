package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// FIXME: Replace this with the "all_*.sql" equivalents. email_addresses doesn't cover enough cases.
func TestParseSchema(t *testing.T) {
	createTable := MustReadFile("test_data/email_addresses_schema.sql")
	schema := ParseSchema(createTable)

	assert.Equal(t, "email_addresses", schema.Name)
	assert.Equal(t, 9, len(schema.Columns))
	assert.Equal(t, Column{"id", "bigint", 20, 0, false, false}, schema.Columns[0])
	assert.Equal(t, Column{"name", "varchar", 19, 0, true, true}, schema.Columns[1])
	assert.Equal(t, Column{"address", "varchar", 255, 0, true, true}, schema.Columns[2])
	assert.Equal(t, Column{"contact_id", "bigint", 20, 0, false, true}, schema.Columns[3])
	assert.Equal(t, Column{"created_at", "datetime", 0, 0, true, true}, schema.Columns[4])
	assert.Equal(t, Column{"updated_at", "datetime", 0, 0, true, true}, schema.Columns[5])
	assert.Equal(t, Column{"import_id", "bigint", 20, 0, false, true}, schema.Columns[6])
	assert.Equal(t, Column{"default_email", "tinyint", 1, 0, true, true}, schema.Columns[7])
	assert.Equal(t, Column{"account_id", "bigint", 20, 0, false, true}, schema.Columns[8])
}

// FIXME: Replace this with the "all_*.sql" equivalents. email_addresses doesn't cover enough cases.
func TestTableSchemaGoType(t *testing.T) {
	createTable := MustReadFile("test_data/email_addresses_schema.sql")
	schema := ParseSchema(createTable)
	goType := schema.GoType()

	assert.Equal(t, reflect.TypeOf(schema), goType.Field(0).Type)
	assert.Equal(t, reflect.TypeOf([]uint8{}), goType.Field(1).Type)
	assert.Equal(t, reflect.TypeOf(uint64(0)), goType.Field(2).Type)
	assert.Equal(t, reflect.TypeOf(""), goType.Field(3).Type)
	assert.Equal(t, reflect.TypeOf(""), goType.Field(4).Type)
	assert.Equal(t, reflect.TypeOf(uint64(0)), goType.Field(5).Type)
	assert.Equal(t, reflect.TypeOf(time.Time{}), goType.Field(6).Type)
	assert.Equal(t, reflect.TypeOf(time.Time{}), goType.Field(7).Type)
	assert.Equal(t, reflect.TypeOf(uint64(0)), goType.Field(8).Type)
	assert.Equal(t, reflect.TypeOf(false), goType.Field(9).Type)
	assert.Equal(t, reflect.TypeOf(uint64(0)), goType.Field(10).Type)
}
