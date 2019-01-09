package memory

import (
	"sync"
)

// Checkpoint implements a KV store to keep track of processing checkpoints.
type Checkpoint struct {
	kvs sync.Map
}

// NewCheckpoint creates a new checkpoint storage.
func NewCheckpoint() *Checkpoint {
	return &Checkpoint{
		kvs: sync.Map{},
	}
}

// Get gets last checkpoint.
func (c *Checkpoint) Get(key string) (string, error) {
	value, exists := c.kvs.Load(key)
	if !exists {
		return "", nil
	}

	return value.(string), nil
}

// Set sets checkpoint.
func (c *Checkpoint) Set(key, value string) error {
	c.kvs.Store(key, value)

	return nil
}
