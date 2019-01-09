package kinesis

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// AfterRecord is a checkpoint strategy
	// When set it stores checkpoint in every record.
	AfterRecord CheckpointStrategy = iota
	// AfterRecordBatch is a checkpoint strategy
	// When set it stores checkpoint in every record batch.
	AfterRecordBatch
)

// ConsumerOption is the abstract functional-parameter type used for worker configuration.
type ConsumerOption func(*Consumer)

// MessageHandler is the message handler.
type MessageHandler func(Message) error

// CheckpointStrategy checkpoint behaviour.
type CheckpointStrategy = int

// Checkpoint manages last checkpoint.
type Checkpoint interface {
	Get(key string) (string, error)
	Set(key, value string) error
}

// Consumer is a kinesis stream consumer.
type Consumer struct {
	client             kinesisiface.KinesisAPI
	group              string
	stream             string
	handler            MessageHandler
	checkpointStrategy CheckpointStrategy
	streamWatcher      streamWatcher
	runnerFactory      runnerFactory
	iteratorType       string
	logger             Logger
}

// NewConsumer creates a new kinesis consumer
func NewConsumer(config ConsumerConfig, handler MessageHandler, checkpoint Checkpoint, opts ...ConsumerOption) (Consumer, error) {
	config.sanitize()
	if err := config.validate(); err != nil {
		return Consumer{}, errors.Wrap(err, "invalid kinesis configuration")
	}

	if handler == nil {
		return Consumer{}, errors.New("invalid handler")
	}

	c := Consumer{
		stream:             config.Stream,
		group:              config.Group,
		checkpointStrategy: AfterRecordBatch,
		iteratorType:       kinesis.ShardIteratorTypeTrimHorizon,
		logger:             DumbLogger,
	}

	for _, opt := range opts {
		opt(&c)
	}

	if c.client == nil {
		client, err := NewClient(config.AWS)
		if err != nil {
			return Consumer{}, errors.Wrap(err, "failed to create a kinesis client")
		}

		c.client = client
	}

	c.streamWatcher = streamWatcher{
		stream:           config.Stream,
		tick:             config.StreamCheckTick,
		client:           c.client,
		deletingCallback: c.onStreamDelete,
		logger:           c.logger,
	}

	c.runnerFactory = runnerFactory{
		runners:            map[string]*runner{},
		client:             c.client,
		group:              config.Group,
		stream:             config.Stream,
		checkpoint:         checkpoint,
		handler:            handler,
		tick:               config.RunnerFactoryTick,
		runnerTick:         config.RunnerTick,
		checkpointStrategy: c.checkpointStrategy,
		runnerIteratorType: c.iteratorType,
		logger:             c.logger,
	}

	return c, nil
}

// WithCheckpointStrategy allows you to configure checkpoint strategy.
func WithCheckpointStrategy(strategy CheckpointStrategy) ConsumerOption {
	return func(r *Consumer) {
		r.checkpointStrategy = strategy
	}
}

// WithClient allows you to set a kinesis client.
func WithClient(client kinesisiface.KinesisAPI) ConsumerOption {
	return func(r *Consumer) {
		if client != nil {
			r.client = client
		}
	}
}

// WithLogger allows you to configure the logger.
func WithLogger(logger Logger) ConsumerOption {
	return func(r *Consumer) {
		if logger != nil {
			r.logger = logger
		}
	}
}

// SinceLatest allows you to set kinesis iterator.
// Start reading just after the most recent record in the shard
func SinceLatest() ConsumerOption {
	return func(r *Consumer) {
		r.iteratorType = kinesis.ShardIteratorTypeLatest
	}
}

// SinceOldest allows you to set kinesis iterator.
// Start reading at the last untrimmed record in the shard in the system
func SinceOldest() ConsumerOption {
	return func(r *Consumer) {
		r.iteratorType = kinesis.ShardIteratorTypeTrimHorizon
	}
}

func (c *Consumer) onStreamDelete() {
	c.Stop()
}

// Start starts the consumer.
func (c *Consumer) Start() error {
	var g errgroup.Group

	c.logger(Info, "starting stream watcher", nil)
	g.Go(c.streamWatcher.Start)

	c.logger(Info, "starting runner factory", nil)
	g.Go(c.runnerFactory.Start)

	if err := g.Wait(); err != nil {
		c.Stop()
		return errors.Wrap(err, "failed to start consumer")
	}

	return nil
}

// Stop stops consumer.
func (c *Consumer) Stop() error {
	var g errgroup.Group

	c.logger(Info, "stopping runner factory", nil)
	g.Go(c.runnerFactory.Stop)

	c.logger(Info, "stopping stream watcher", nil)
	g.Go(c.streamWatcher.Stop)

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "failed to stop consumer")
	}

	return nil
}
