package redis

import (
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// Checkpoint implements a KV store to keep track of processing checkpoints.
type Checkpoint struct {
	redis *redis.Client
}

// NewCheckpoint creates a new checkpoint storage.
func NewCheckpoint(redis *redis.Client) Checkpoint {
	return Checkpoint{
		redis: redis,
	}
}

// Get the key/value.
func (c Checkpoint) Get(key string) (string, error) {
	value, err := c.redis.Get("kinesis:" + key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}

		return "", errors.Wrap(err, "error during query by a key")
	}

	return value, nil
}

// Set the key/value.
func (c Checkpoint) Set(key, value string) error {
	if err := c.redis.Set("kinesis:"+key, value, 0).Err(); err != nil {
		return errors.New("error during set the key")
	}

	return nil
}
