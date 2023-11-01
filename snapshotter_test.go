package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotterExcludesTables(t *testing.T) {
	SetFakeResponses(
		FakeMysqlResponse{  // SHOW TABLES
			false,
			[]string{"Tables_in_honk"},
			[][]any{{"foo"}, {"honk"}, {"bonk"}},
		},
	)

	WithConfig("EXCLUDE_TABLES", "honk", func() {
		tables := getTableList()
		assert.Equal(t, []string{"foo", "bonk"}, tables)
	})
}
