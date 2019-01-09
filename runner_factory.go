package kinesis

import (
	"fmt"
	"time"

	"gitlab.com/marcoxavier/go-kinesis/internal/worker"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// runnerFactory periodic checks shard status an notifies if is deleting.
type runnerFactory struct {
	worker.Worker
	runners            map[string]*runner
	handler            MessageHandler
	checkpoint         Checkpoint
	group              string
	stream             string
	tick               time.Duration
	runnerTick         time.Duration
	runnerIteratorType string
	checkpointStrategy CheckpointStrategy
	client             kinesisiface.KinesisAPI
	logger             Logger
}

func (f *runnerFactory) checkShards() error {
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(f.stream),
	}

	f.logger(Debug, "fetching shards", nil)
	result, err := f.client.ListShards(input)
	if err != nil {
		f.logger(Error, "failed to fetch shards", LoggerData{"cause": fmt.Sprintf("%v", err)})
		return nil
	}

	for _, shard := range result.Shards {
		if _, exists := f.runners[*shard.ShardId]; exists {
			continue
		}

		f.logger(Debug, "creating new runner", LoggerData{"shard_id": *shard.ShardId})
		r := runner{
			client:             f.client,
			handler:            f.handler,
			shardID:            *shard.ShardId,
			group:              f.group,
			stream:             f.stream,
			checkpoint:         f.checkpoint,
			checkpointStrategy: f.checkpointStrategy,
			tick:               f.runnerTick,
			iteratorType:       f.runnerIteratorType,
			logger:             f.logger,
		}

		f.runners[*shard.ShardId] = &r

		f.logger(Info, "starting runner", LoggerData{"shard_id": *shard.ShardId})
		go func() {
			// TODO proper handler this error
			if err := r.Start(); err != nil {
				f.logger(Error, "failed start runner", LoggerData{"shard_id": *shard.ShardId})
			}
		}()
	}

	return nil
}

// Start starts runner factory.
func (f *runnerFactory) Start() error {
	notifier := worker.NewCronNotifier(f.tick)
	f.Worker = worker.NewWorker(f.checkShards, worker.WithNotifier(notifier), worker.WithLogger(f.logger))

	return f.Worker.Start()
}

// Stop stops runner factory.
func (f *runnerFactory) Stop() error {
	if err := f.Worker.Stop(); err != nil {
		return err
	}

	var g errgroup.Group

	f.logger(Info, "stopping runners", nil)
	for _, runner := range f.runners {
		r := runner
		g.Go(r.Stop)
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "failed to stop runner factory")
	}

	return nil
}
