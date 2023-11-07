package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// FIXME: Add a retry to the Redis calls.

const REDIS_TIMEOUT = 5 * time.Second

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
	if InTestMode() {
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

// Returns an empty string if the key doesn't exist.
func (ssr *StateStorageRedis) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), REDIS_TIMEOUT, errors.New("Redis get timeout"))
	defer cancel()

	s, err := ssr.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	} else if err != nil {
		return "", err
	} else {
		return s, nil
	}
}

func (ssr *StateStorageRedis) Set(key string, value string) error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), REDIS_TIMEOUT, errors.New("Redis set timeout"))
	defer cancel()
	return ssr.Client.Set(ctx, key, value, 0).Err()
}

func (ssr *StateStorageRedis) Delete(key string) error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), REDIS_TIMEOUT, errors.New("Redis del timeout"))
	defer cancel()
	return ssr.Client.Del(ctx, key).Err()
}

func (ssr *StateStorageRedis) ClearAll() error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), REDIS_TIMEOUT, errors.New("Redis flushall timeout"))
	defer cancel()
	return ssr.Client.FlushAll(ctx).Err()
}
