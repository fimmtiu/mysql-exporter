package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func runStateStorageTest(t *testing.T, storage StateStorage) {
	s, err := storage.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "", s)

	err = storage.Set("foo", "honk")
	assert.NoError(t, err)
	s, err = storage.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "honk", s)

	s, err = storage.Get("bar")
	assert.NoError(t, err)
	assert.Equal(t, "", s)

	err = storage.Set("bar", "bonk")
	assert.NoError(t, err)

	err = storage.Delete("foo")
	assert.NoError(t, err)
	s, err = storage.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "", s)

	s, err = storage.Get("bar")
	assert.NoError(t, err)
	assert.Equal(t, "bonk", s)

	err = storage.ClearAll()
	assert.NoError(t, err)

	s, err = storage.Get("bar")
	assert.NoError(t, err)
	assert.Equal(t, "", s)
}

func TestStateStorage(t *testing.T) {
	runStateStorageTest(t, NewStateStorageMemory())
}

func TestStateStorageIntegration(t *testing.T) {
	WithIntegrationTestSetup(func() {
		runStateStorageTest(t, NewStateStorageRedis())
	})
}
