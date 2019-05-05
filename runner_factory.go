package kinesis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// Runner is a shard iterator.
type Runner interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	ShardID() string
	Closed() bool
	RestartCursor()
}

// runnerFactory periodic checks shard status an notifies if is deleting.
type runnerFactory struct {
	runners     sync.Map
	handler     MessageHandler
	config      ConsumerConfig
	options     ConsumerOptions
	checkpoint  Checkpoint
	client      kinesisiface.KinesisAPI
	logger      Logger
	eventLogger EventLogger
}

func (f *runnerFactory) checkShards(ctx context.Context) error {
	start := time.Now()
	f.eventLogger.LogEvent(EventLog{Event: ShardManagerTriggered, Elapse: time.Now().Sub(start)})

	input := &kinesis.ListShardsInput{
		StreamName: aws.String(f.config.Stream),
	}

	f.logger.Log(LevelDebug, nil, "fetching shards")
	result, err := f.client.ListShards(input)
	if err != nil {
		f.logger.Log(LevelError, loggerData{"cause": fmt.Sprintf("%v", err)}, "failed to fetch shards")
		return nil
	}

	for _, shard := range result.Shards {
		if _, exists := f.runners.Load(aws.StringValue(shard.ShardId)); exists {
			continue
		}

		if !f.shouldStart(shard) {
			f.logger.Log(
				LevelDebug,
				loggerData{"shard_id": aws.StringValue(shard.ShardId), "parent_shard_id": aws.StringValue(shard.ParentShardId), "adjacent_shard_id": aws.StringValue(shard.AdjacentParentShardId)},
				"runner not eligible to start",
			)
			continue
		}

		f.logger.Log(LevelDebug, loggerData{"shard_id": aws.StringValue(shard.ShardId)}, "creating new runner")
		iteratorConfig, exists := f.options.iterators[aws.StringValue(shard.ShardId)]
		if !exists {
			iteratorConfig = ConsumerIterator{
				ShardID: aws.StringValue(shard.ShardId),
				Type:    f.options.iteratorType,
			}
		}

		r := &runner{
			client:         f.client,
			handler:        f.handler,
			shardID:        aws.StringValue(shard.ShardId),
			options:        f.options,
			config:         f.config,
			checkpoint:     f.checkpoint,
			logger:         f.logger,
			eventLogger:    f.eventLogger,
			stopped:        make(chan struct{}),
			iteratorConfig: iteratorConfig,
		}

		f.runners.Store(aws.StringValue(shard.ShardId), r)

		go func() {
			// TODO proper handle this error
			f.logger.Log(LevelInfo, loggerData{"shard_id": r.ShardID()}, "starting runner")
			if err := r.Start(ctx); err != nil {
				f.logger.Log(LevelError, loggerData{"shard_id": r.ShardID()}, "failed start runner")
			}
		}()
	}

	return nil
}

func (f *runnerFactory) shouldStart(shard *kinesis.Shard) bool {
	if f.options.specificShards != nil {
		if exists := f.options.specificShards[aws.StringValue(shard.ShardId)]; !exists {
			return false
		}
	}

	if f.options.skipReshardingOrder {
		return true
	}

	parentRunner, exists := f.runners.Load(aws.StringValue(shard.ParentShardId))
	if runner, _ := parentRunner.(Runner); exists && !runner.Closed() {
		return false
	}

	adjacentRunner, exists := f.runners.Load(aws.StringValue(shard.AdjacentParentShardId))
	if runner, _ := adjacentRunner.(Runner); exists && !runner.Closed() {
		return false
	}

	return true
}

func (f *runnerFactory) stopRunners() error {
	ctx := context.Background()

	f.runners.Range(func(key, value interface{}) bool {
		r := value.(Runner)

		f.logger.Log(LevelInfo, loggerData{"shard_id": r.ShardID()}, "stopping runner")
		if err := r.Stop(ctx); err != nil {
			f.logger.Log(LevelError, loggerData{"shard_id": r.ShardID(), "cause": fmt.Sprintf("%v", err)}, "failed to stop runner")
		}

		return true
	})

	return nil
}

// Run runs runner factory.
func (f *runnerFactory) Run(ctx context.Context) error {
	ticker := time.NewTicker(f.config.RunnerFactoryTick)
	defer ticker.Stop()

	for {
		if err := f.checkShards(ctx); err != nil {
			return errors.Wrap(err, "failed to check shards")
		}

		select {
		case <-ctx.Done():
			return f.stopRunners()
		case <-ticker.C:
			continue
		}
	}
}

// ResetCursors resets all runners cursor.
func (f *runnerFactory) ResetCursors() {
	f.runners.Range(func(key, value interface{}) bool {
		runner := value.(Runner)
		if !runner.Closed() {
			runner.RestartCursor()
		}

		return true
	})
}
