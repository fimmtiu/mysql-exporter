package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSyntheticColumns(t *testing.T) {
	os.Setenv("SYNTHETIC_COLUMNS", "foo,int,1;bar,char(4),'honk, woop'")
	defer os.Unsetenv("SYNTHETIC_COLUMNS")

	config := NewConfig()

	assert.True(t, len(config.SyntheticColumns) == 2)
	assert.Equal(t, "foo", config.SyntheticColumns[0])
	assert.Equal(t, "bar", config.SyntheticColumns[1])

	assert.True(t, len(config.SyntheticColumnTypes) == 2)
	assert.True(t, config.SyntheticColumnTypes["foo"] == "int")
	assert.True(t, config.SyntheticColumnTypes["bar"] == "char(4)")

	assert.True(t, config.SyntheticColumnNames == "foo,bar")
	assert.True(t, config.SyntheticColumnValues == "1,'honk, woop'")
}
