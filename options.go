package kinesis

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// ConsumerOption is the abstract functional-parameter type used for worker configuration.
type ConsumerOption func(*ConsumerOptions)

type ConsumerOptions struct {
	client              kinesisiface.KinesisAPI
	iteratorType        string
	skipReshardingOrder bool
	checkpointStrategy  CheckpointStrategy
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

// SinceLatest allows you to set kinesis iterator.
// Start reading just after the most recent record in the shard
func SinceLatest() ConsumerOption {
	return func(c *ConsumerOptions) {
		c.iteratorType = kinesis.ShardIteratorTypeLatest
	}
}

// SinceOldest allows you to set kinesis iterator.
// Start reading at the last untrimmed record in the shard in the system
func SinceOldest() ConsumerOption {
	return func(c *ConsumerOptions) {
		c.iteratorType = kinesis.ShardIteratorTypeTrimHorizon
	}
}

// SkipReshardingOrder allows you to set consumer to start reading shards since detected.
func SkipReshardingOrder() ConsumerOption {
	return func(c *ConsumerOptions) {
		c.skipReshardingOrder = true
	}
}
