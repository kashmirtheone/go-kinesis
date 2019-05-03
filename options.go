package kinesis

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// ConsumerOption is the abstract functional-parameter type used for worker configuration.
type ConsumerOption func(*ConsumerOptions)

// ConsumerOptions holds all consumer options.
type ConsumerOptions struct {
	client              kinesisiface.KinesisAPI
	iteratorType        IteratorType
	iterators           map[string]ConsumerIterator
	specificShards      map[string]bool
	skipReshardingOrder bool
	checkpointStrategy  CheckpointStrategy
}

// ConsumerIterator is a iterator configuration by shard.
type ConsumerIterator struct {
	Type     IteratorType
	ShardID  string
	Sequence string
}

func (i *ConsumerIterator) getType() string {
	switch i.Type {
	case IteratorTypeSequence:
		return kinesis.ShardIteratorTypeAtSequenceNumber
	case IteratorTypeAfterSequence:
		return kinesis.ShardIteratorTypeAfterSequenceNumber
	case IteratorTypeTail:
		return kinesis.ShardIteratorTypeLatest
	case IteratorTypeHead:
		return kinesis.ShardIteratorTypeTrimHorizon
	}

	return kinesis.ShardIteratorTypeTrimHorizon
}

// WithCheckpointStrategy allows you to configure checkpoint strategy.
func WithCheckpointStrategy(strategy CheckpointStrategy) ConsumerOption {
	return func(c *ConsumerOptions) {
		c.checkpointStrategy = strategy
	}
}

// WithClient allows you to set a kinesis client.
func WithClient(client kinesisiface.KinesisAPI) ConsumerOption {
	return func(c *ConsumerOptions) {
		if client != nil {
			c.client = client
		}
	}
}

// WithSpecificIterators allows you to set a specific iterators per shard.
func WithSpecificIterators(iterators map[string]ConsumerIterator) ConsumerOption {
	return func(c *ConsumerOptions) {
		if iterators != nil {
			c.iterators = iterators
		}
	}
}

// WithSpecificShards allows you to set a filtered shards.
// Consumer only reads specified shards ids.
func WithSpecificShards(shardIDs []string) ConsumerOption {
	return func(c *ConsumerOptions) {
		if shardIDs != nil {
			c.specificShards = make(map[string]bool)
			for _, shardID := range shardIDs {
				c.specificShards[shardID] = true
			}
		}
	}
}

// SinceLatest allows you to set kinesis iterator.
// Starts reading just after the most recent record in the shard
func SinceLatest() ConsumerOption {
	return func(c *ConsumerOptions) {
		c.iteratorType = IteratorTypeTail
	}
}

// SinceOldest allows you to set kinesis iterator.
// Starts reading at the last untrimmed record in the shard in the system
func SinceOldest() ConsumerOption {
	return func(c *ConsumerOptions) {
		c.iteratorType = IteratorTypeHead
	}
}

// SinceSequence allows you to set kinesis iterator.
// Starts reading since sequence number in a specific shard.
func SinceSequence(shardID string, sequence string) ConsumerOption {
	return func(c *ConsumerOptions) {
		c.iterators[shardID] = ConsumerIterator{
			Type:     IteratorTypeSequence,
			ShardID:  shardID,
			Sequence: sequence,
		}

		c.specificShards = map[string]bool{shardID: true}
	}
}

// SkipReshardingOrder allows you to set consumer to start reading shards since detected.
func SkipReshardingOrder() ConsumerOption {
	return func(c *ConsumerOptions) {
		c.skipReshardingOrder = true
	}
}
