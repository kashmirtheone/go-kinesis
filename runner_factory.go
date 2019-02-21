package kinesis

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// runnerFactory periodic checks shard status an notifies if is deleting.
type runnerFactory struct {
	runners             map[string]*runner
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
}

func (f *runnerFactory) checkShards(ctx context.Context) error {
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(f.stream),
	}

	log(Debug, nil, "fetching shards")
	result, err := f.client.ListShardsWithContext(ctx, input)
	if err != nil {
		log(Error, loggerData{"cause": fmt.Sprintf("%v", err)}, "failed to fetch shards")
		return nil
	}

	for _, shard := range result.Shards {
		if _, exists := f.runners[*shard.ShardId]; exists {
			continue
		}

		if !f.skipReshardingOrder && !f.eligibleToStart(shard) {
			log(
				Debug,
				loggerData{"shard_id": *shard.ShardId, "parent_shard_id": *shard.ParentShardId, "adjacent_shard_id": *shard.AdjacentParentShardId},
				"runner not eligible to start, waiting for parent runners to close",
			)
			continue
		}

		log(Debug, loggerData{"shard_id": *shard.ShardId}, "creating new runner")
		r := &runner{
			client:             f.client,
			handler:            f.handler,
			shardID:            *shard.ShardId,
			group:              f.group,
			stream:             f.stream,
			checkpoint:         f.checkpoint,
			checkpointStrategy: f.checkpointStrategy,
			tick:               f.runnerTick,
			iteratorType:       f.runnerIteratorType,
		}

		f.runners[*shard.ShardId] = r

		go func() {
			// TODO proper handler this error
			log(Info, loggerData{"shard_id": r.shardID}, "starting runner")
			if err := r.Start(ctx); err != nil {
				log(Error, loggerData{"shard_id": r.shardID}, "failed start runner")
			}
		}()
	}

	return nil
}

func (f *runnerFactory) eligibleToStart(shard *kinesis.Shard) bool {
	if shard.ParentShardId != nil {
		parentRunner, exists := f.runners[*shard.ParentShardId]
		if exists && !parentRunner.Closed() {
			return false
		}
	}

	if shard.AdjacentParentShardId != nil {
		parentRunner, exists := f.runners[*shard.AdjacentParentShardId]
		if exists && !parentRunner.Closed() {
			return false
		}
	}

	return true
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
			return nil
		case <-ticker.C:
			continue
		}
	}
}
