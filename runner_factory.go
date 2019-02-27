package kinesis

import (
	"context"
	"fmt"
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
}

// runnerFactory periodic checks shard status an notifies if is deleting.
type runnerFactory struct {
	runners             map[string]Runner
	handler             MessageHandler
	checkpoint          Checkpoint
	group               string
	stream              string
	tick                time.Duration
	runnerTick          time.Duration
	runnerIteratorType  string
	checkpointStrategy  CheckpointStrategy
	client              kinesisiface.KinesisAPI
	skipReshardingOrder bool
	logger              Logger
	eventLogger         EventLogger
}

func (f *runnerFactory) checkShards(ctx context.Context) error {
	start := time.Now()
	f.eventLogger.LogEvent(EventLog{Event: ShardManagerTriggered, Elapse: time.Now().Sub(start)})

	input := &kinesis.ListShardsInput{
		StreamName: aws.String(f.stream),
	}

	f.logger.Log(LevelDebug, nil, "fetching shards")
	result, err := f.client.ListShardsWithContext(ctx, input)
	if err != nil {
		f.logger.Log(LevelError, loggerData{"cause": fmt.Sprintf("%v", err)}, "failed to fetch shards")
		return nil
	}

	for _, shard := range result.Shards {
		if _, exists := f.runners[aws.StringValue(shard.ShardId)]; exists {
			continue
		}

		if !f.shouldStart(shard) {
			f.logger.Log(
				LevelDebug,
				loggerData{"shard_id": aws.StringValue(shard.ShardId), "parent_shard_id": aws.StringValue(shard.ParentShardId), "adjacent_shard_id": aws.StringValue(shard.AdjacentParentShardId)},
				"runner not eligible to start, waiting for parent runners to close",
			)
			continue
		}

		f.logger.Log(LevelDebug, loggerData{"shard_id": aws.StringValue(shard.ShardId)}, "creating new runner")
		r := &runner{
			client:             f.client,
			handler:            f.handler,
			shardID:            aws.StringValue(shard.ShardId),
			group:              f.group,
			stream:             f.stream,
			checkpoint:         f.checkpoint,
			checkpointStrategy: f.checkpointStrategy,
			tick:               f.runnerTick,
			iteratorType:       f.runnerIteratorType,
			logger:             f.logger,
			eventLogger:        f.eventLogger,
			stopped:            make(chan struct{}),
		}

		f.runners[aws.StringValue(shard.ShardId)] = r

		go func() {
			// TODO proper handler this error
			f.logger.Log(LevelInfo, loggerData{"shard_id": r.ShardID()}, "starting runner")
			if err := r.Start(ctx); err != nil {
				f.logger.Log(LevelError, loggerData{"shard_id": r.ShardID()}, "failed start runner")
			}
		}()
	}

	return nil
}

func (f *runnerFactory) shouldStart(shard *kinesis.Shard) bool {
	if f.skipReshardingOrder {
		return true
	}

	parentRunner, exists := f.runners[aws.StringValue(shard.ParentShardId)]
	if exists && !parentRunner.Closed() {
		return false
	}

	adjacentRunner, exists := f.runners[aws.StringValue(shard.AdjacentParentShardId)]
	if exists && !adjacentRunner.Closed() {
		return false
	}

	return true
}

func (f *runnerFactory) stopRunners() error {
	ctx := context.Background()

	for i := range f.runners {
		r := f.runners[i]

		f.logger.Log(LevelInfo, loggerData{"shard_id": r.ShardID()}, "stopping runner")
		if err := r.Stop(ctx); err != nil {
			f.logger.Log(LevelError, loggerData{"shard_id": r.ShardID(), "cause": fmt.Sprintf("%v", err)}, "failed to stop runner")
		}
	}

	return nil
}

// Run runs runner factory.
func (f *runnerFactory) Run(ctx context.Context) error {
	ticker := time.NewTicker(f.tick)
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
