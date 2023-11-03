package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteFromSlice(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5, 6, 7}

	slice = DeleteFromSlice(slice, 2)
	assert.Equal(t, 6, len(slice))
	assert.Equal(t, []int{1, 2, 4, 5, 6, 7}, slice)

	slice = DeleteFromSlice(slice, 0)
	assert.Equal(t, 5, len(slice))
	assert.Equal(t, []int{2, 4, 5, 6, 7}, slice)

	slice = DeleteFromSlice(slice, 4)
	assert.Equal(t, 4, len(slice))
	assert.Equal(t, []int{2, 4, 5, 6}, slice)
}

func TestFormatEpochDate(t *testing.T) {
	assert.Equal(t, "1970-01-01", FormatEpochDate(0))
	assert.Equal(t, "1970-01-02", FormatEpochDate(1))
	assert.Equal(t, "2022-12-08", FormatEpochDate(19334))
	assert.Equal(t, "2022-12-09", FormatEpochDate(19335))
}

func TestFormatMillisecondTime(t *testing.T) {
	assert.Equal(t, "00:00:00", FormatMillisecondTime(0))
	assert.Equal(t, "00:00:01", FormatMillisecondTime(1000))
	assert.Equal(t, "13:52:49", FormatMillisecondTime(49969098))
	assert.Equal(t, "23:41:30", FormatMillisecondTime(85290923))
}
