package kinesis

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
)

const (
	// AfterRecord is a checkpoint strategy
	// When set it stores checkpoint in every record.
	AfterRecord CheckpointStrategy = iota
	// AfterRecordBatch is a checkpoint strategy
	// When set it stores checkpoint in every record batch.
	AfterRecordBatch
)

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
	ConsumerOptions
	group         string
	stream        string
	handler       MessageHandler
	streamWatcher *streamWatcher
	runnerFactory *runnerFactory
}

// NewConsumer creates a new kinesis consumer
func NewConsumer(config ConsumerConfig, handler MessageHandler, checkpoint Checkpoint, opts ...ConsumerOption) (*Consumer, error) {
	config.sanitize()
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid kinesis configuration")
	}

	if handler == nil {
		return nil, errors.New("invalid handler")
	}

	c := &Consumer{
		ConsumerOptions: ConsumerOptions{
			checkpointStrategy: AfterRecordBatch,
			iteratorType:       kinesis.ShardIteratorTypeTrimHorizon,
		},
		stream: config.Stream,
		group:  config.Group,
	}

	for _, opt := range opts {
		opt(&c.ConsumerOptions)
	}

	if c.client == nil {
		client, err := NewClient(config.AWS)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create a kinesis client")
		}

		c.client = client
	}

	c.streamWatcher = &streamWatcher{
		stream: config.Stream,
		tick:   config.StreamCheckTick,
		client: c.client,
	}

	c.runnerFactory = &runnerFactory{
		runners:             map[string]*runner{},
		client:              c.client,
		group:               config.Group,
		stream:              config.Stream,
		checkpoint:          checkpoint,
		handler:             handler,
		tick:                config.RunnerFactoryTick,
		runnerTick:          config.RunnerTick,
		checkpointStrategy:  c.checkpointStrategy,
		runnerIteratorType:  c.iteratorType,
		skipReshardingOrder: c.skipReshardingOrder,
	}

	return c, nil
}

// SetLogger allows you to set the logger.
func (c *Consumer) SetLogger(logger Logger) {
	if logger != nil {
		log = logger
	}
}

// Run runs the consumer.
func (c *Consumer) Run(ctx context.Context) error {
	inCtx, cancel := context.WithCancel(ctx)
	c.streamWatcher.deletingCallback = cancel

	errChan := make(chan error, 2)
	wg := sync.WaitGroup{}
	var err error
	wg.Add(2)

	go func() {
		defer wg.Done()
		log(Info, nil, "starting stream watcher")
		if err := c.streamWatcher.Run(inCtx); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		log(Info, nil, "starting runner factory")
		if err := c.runnerFactory.Run(inCtx); err != nil {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		break
	case err = <-errChan:
		cancel()
	}

	wg.Wait()

	if err != nil {
		return errors.Wrap(err, "consumer terminated with errors")
	}

	return nil
}
