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
