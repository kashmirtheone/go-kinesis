package kinesis

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	runnerTick = time.Second
)

// runner is a single goroutine capable to listen a shard.
type runner struct {
	client             kinesisiface.KinesisAPI
	handler            MessageHandler
	shardID            string
	group              string
	stream             string
	tick               time.Duration
	checkpoint         Checkpoint
	checkpointStrategy CheckpointStrategy
	iteratorType       string
	shutdown           context.CancelFunc
	closed             bool
}

func (r *runner) Start(ctx context.Context) error {
	mCtx, cancel := context.WithCancel(ctx)
	r.shutdown = cancel

	ticker := time.NewTicker(r.tick)
	defer ticker.Stop()

	for {
		if err := r.process(mCtx); err != nil {
			return errors.Wrap(err, "failed to process shard")
		}

		select {
		case <-mCtx.Done():
			return nil
		case <-ticker.C:
			continue
		}
	}
}

func (r *runner) Stop() error {
	r.shutdown()
	return nil
}

// Closed return if runner's shard is closed.
func (r *runner) Closed() bool {
	return r.closed
}

func (r *runner) process(ctx context.Context) error {
	log(Debug, nil, "getting last checkpoint")
	lastSequence, err := r.checkpoint.Get(r.checkpointIdentifier())
	if err != nil {
		log(Error, nil, "failed getting sequence number", loggerData{"cause": fmt.Sprintf("%v", err)})
		return nil
	}

	log(Debug, nil, "getting shard iterator")
	shardIterator, err := r.getShardIterator(ctx, lastSequence)
	if err != nil {
		log(Error, loggerData{"cause": fmt.Sprintf("%v", err)}, "error getting shard iterator")
		return nil
	}

	// AWS documentation recommends GetRecords to be rate limited to 1 seconds to avoid
	// ProvisionedThroughputExceededExceptions.
	ticker := time.NewTicker(runnerTick)
	defer ticker.Stop() // nolint

	for {
		log(Debug, nil, "getting records")
		resp, err := r.client.GetRecordsWithContext(ctx, &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if ok && aerr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException {
				log(Info, nil, "the request rate for the stream is too high or the requested Data is too large for the available throughput, waiting...")

				return nil
			}

			log(Error, loggerData{"cause": fmt.Sprintf("%v", err)}, "error getting records")

			return nil
		}

		if resp.NextShardIterator == nil {
			log(Info, nil, "shard is closed, stopping runner")
			r.closed = true
			r.shutdown()

			return nil
		}

		if len(resp.Records) <= 0 {
			log(Debug, nil, "there is no records to process, jumping to next iteration")
			shardIterator = resp.NextShardIterator

			goto next
		}

		log(Debug, nil, "processing records")
		for _, record := range resp.Records {
			if err := r.processRecord(record); err != nil {
				resp.NextShardIterator, err = r.getShardIterator(ctx, lastSequence)
				if err != nil {
					log(Error, loggerData{"cause": fmt.Sprintf("%v", err)}, "error getting shard iterator")
					return nil
				}

				log(Error, loggerData{"cause": fmt.Sprintf("%v", err)}, "error handling message")

				break
			}

			lastSequence = *record.SequenceNumber

			if r.checkpointStrategy == AfterRecord {
				log(Debug, nil, "setting checkpoint")
				if err := r.checkpoint.Set(r.checkpointIdentifier(), lastSequence); err != nil {
					log(Error, loggerData{"cause": fmt.Sprintf("%v", err)}, "error setting sequence number")

					return nil
				}
			}
		}

		if r.checkpointStrategy == AfterRecordBatch {
			if err := r.checkpoint.Set(r.checkpointIdentifier(), lastSequence); err != nil {
				log(Error, loggerData{"cause": fmt.Sprintf("%v", err)}, "error setting sequence number")

				return nil
			}
		}

		shardIterator = resp.NextShardIterator
		log(Debug, nil, "records processed")

	next:
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			continue
		}
	}
}

func (r *runner) processRecord(record *kinesis.Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = errors.Wrap(fmt.Errorf("%s", p), "runner terminated due a panic")
		}
	}()

	message := Message{Partition: *record.PartitionKey, Data: record.Data}

	if err := r.handler(message); err != nil {
		return errors.Wrap(err, "error handling message")
	}

	return nil
}

func (r *runner) getShardIterator(ctx context.Context, sequence string) (*string, error) {
	getShardOptions := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(r.shardID),
		StreamName:        aws.String(r.stream),
		ShardIteratorType: aws.String(r.iteratorType),
	}

	if sequence != "" {
		getShardOptions.
			SetStartingSequenceNumber(sequence).
			SetShardIteratorType(kinesis.ShardIteratorTypeAfterSequenceNumber)
	}

	iteratorOutput, err := r.client.GetShardIteratorWithContext(ctx, getShardOptions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get shard iterator")
	}

	return iteratorOutput.ShardIterator, nil
}

func (r *runner) checkpointIdentifier() string {
	return fmt.Sprintf("%s_%s_%s", r.stream, r.group, r.shardID)
}
