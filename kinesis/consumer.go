package kinesis

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

const (
	// AfterRecord is a checkpoint strategy
	// When set it stores checkpoint in every record.
	AfterRecord CheckpointStrategy = iota
	// AfterRecordBatch is a checkpoint strategy
	// When set it stores checkpoint in every record batch.
	AfterRecordBatch
	// IteratorTypeTail is a iterator type that defines that consumer starts reading from tail.
	IteratorTypeTail IteratorType = iota
	// IteratorTypeHead is a iterator type that defines that consumer starts reading from beginning.
	IteratorTypeHead
	// IteratorTypeSequence is a iterator type that defines that consumer starts reading from a sequence number.
	IteratorTypeSequence
	// IteratorTypeAfterSequence is a iterator type that defines that consumer starts reading from a sequence number + 1.
	IteratorTypeAfterSequence
)

// StreamChecker checks stream state and handles it.
type StreamChecker interface {
	Run(ctx context.Context) error
	SetDeletingCallback(cb func())
}

// RunnerFactory handler stream sharding.
type RunnerFactory interface {
	Run(ctx context.Context) error
	ResetCursors()
}

// MessageHandler is the message handler.
type MessageHandler func(ctx context.Context, msg Message) error

// CheckpointStrategy checkpoint behaviour.
type CheckpointStrategy = int

// IteratorType iterator type.
type IteratorType = int

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
	streamWatcher StreamChecker
	runnerFactory RunnerFactory
	logger        Logger
	eventLogger   EventLogger
	stats         ConsumerStats
	stopped       chan struct{}
	shutdown      chan struct{}
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
			iteratorType:       IteratorTypeHead,
			iterators:          make(map[string]ConsumerIterator),
		},
		stream:      config.Stream,
		group:       config.Group,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stats:       ConsumerStats{},
		stopped:     make(chan struct{}),
		shutdown:    make(chan struct{}),
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
		client:      c.client,
		config:      config,
		logger:      c,
		eventLogger: c.eventLogger,
	}

	f := &runnerFactory{
		runners:     sync.Map{},
		client:      c.client,
		config:      config,
		options:     c.ConsumerOptions,
		checkpoint:  checkpoint,
		handler:     handler,
		logger:      c,
		eventLogger: c,
	}

	f.factory = func(shardID string, iteratorConfig ConsumerIterator) Runner {
		return &runner{
			client:         f.client,
			handler:        f.handler,
			shardID:        shardID,
			options:        f.options,
			config:         f.config,
			checkpoint:     f.checkpoint,
			logger:         f.logger,
			eventLogger:    f.eventLogger,
			stopped:        make(chan struct{}),
			iteratorConfig: iteratorConfig,
		}
	}

	c.runnerFactory = f

	return c, nil
}

// Stats returns consumer stats.
func (c *Consumer) Stats() ConsumerStats {
	return c.stats
}

// Log main logger.
func (c *Consumer) Log(level string, data map[string]interface{}, format string, args ...interface{}) {
	c.logger.Log(level, data, format, args...)
}

// LogEvent main log event.
func (c *Consumer) LogEvent(event EventLog) {
	c.eventLogger.LogEvent(event)
	c.stats.statsHandler(event)
}

// SetLogger allows you to set the logger.
func (c *Consumer) SetLogger(logger Logger) {
	if logger != nil {
		c.logger = logger
	}
}

// SetEventLogger allows you to set the event logger.
func (c *Consumer) SetEventLogger(eventLogger EventLogger) {
	if eventLogger != nil {
		c.eventLogger = eventLogger
	}
}

// ResetIterators resets iterator for active shards.
func (c *Consumer) ResetIterators() error {
	c.logger.Log(LevelInfo, nil, "resetting cursors")
	c.runnerFactory.ResetCursors()
	return nil
}

// Start starts consumer.
func (c *Consumer) Start() error {
	if err := c.Run(context.Background()); err != nil {
		return err
	}

	<-c.stopped
	return nil
}

// Stop stops consumer
func (c *Consumer) Stop() error {
	c.shutdown <- struct{}{}
	<-c.stopped
	return nil
}

// Run runs the consumer.
func (c *Consumer) Run(ctx context.Context) error {
	inCtx, cancel := context.WithCancel(ctx)
	c.streamWatcher.SetDeletingCallback(cancel)

	errChan := make(chan error, 2)
	wg := sync.WaitGroup{}
	var err error
	wg.Add(2)

	go func() {
		defer wg.Done()
		c.logger.Log(LevelInfo, nil, "starting stream watcher")
		if err := c.streamWatcher.Run(inCtx); err != nil {
			errChan <- err
		}
	}()

	go func() {
		defer wg.Done()
		c.logger.Log(LevelInfo, nil, "starting runner factory")
		if err := c.runnerFactory.Run(inCtx); err != nil {
			errChan <- err
		}
	}()

	go func() {
		<-c.shutdown
		cancel()
	}()

	select {
	case <-inCtx.Done():
		break
	case err = <-errChan:
		cancel()
	}

	wg.Wait()

	defer close(c.stopped)
	if err != nil {
		return errors.Wrap(err, "consumer terminated with errors")
	}

	return nil
}
