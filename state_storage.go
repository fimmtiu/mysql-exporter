package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// FIXME: Add a retry to the Redis calls.
// FIXME: Add a timeout to the Redis calls.

type StateStorage interface {
	Get(key string) (string, error)
	Set(key string, val string) error
	Delete(key string) error
	ClearAll() error
}

type StateStorageMemory struct {
	contents map[string]string
}

type StateStorageRedis struct {
  Client *redis.Client
}

func NewStateStorage() StateStorage {
	if InTestMode() && !InIntegrationTestMode() {
		return NewStateStorageMemory()
	} else {
		return NewStateStorageRedis()
	}
}

func NewStateStorageMemory() *StateStorageMemory {
	return &StateStorageMemory{make(map[string]string)}
}

func (ssm *StateStorageMemory) Get(key string) (string, error) {
	return ssm.contents[key], nil
}

func (ssm *StateStorageMemory) Set(key string, val string) error {
	ssm.contents[key] = val
	return nil
}

func (ssm *StateStorageMemory) Delete(key string) error {
	delete(ssm.contents, key)
	return nil
}

func (ssm *StateStorageMemory) ClearAll() error {
	clear(ssm.contents)
	return nil
}

func NewStateStorageRedis() *StateStorageRedis {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", config.RedisHost, config.RedisPort),
		Password: config.RedisPassword,
	})
	return &StateStorageRedis{rdb}
}

func (ssr *StateStorageRedis) Get(key string) (string, error) {
	return ssr.Client.Get(context.Background(), key).Result()
}

func (ssr *StateStorageRedis) Set(key string, value string) error {
	return ssr.Client.Set(context.Background(), key, value, 0).Err()
}

func (ssr *StateStorageRedis) Delete(key string) error {
	return ssr.Client.Del(context.Background(), key).Err()
}

func (ssr *StateStorageRedis) ClearAll() error {
	return ssr.Client.FlushAll(context.Background()).Err()
}

