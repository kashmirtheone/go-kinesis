package kinesis

import (
	"fmt"
	"time"

	"gitlab.com/marcoxavier/go-kinesis/internal/worker"

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
	worker.Worker
	client             kinesisiface.KinesisAPI
	handler            MessageHandler
	shardID            string
	group              string
	stream             string
	tick               time.Duration
	checkpoint         Checkpoint
	checkpointStrategy CheckpointStrategy
	iteratorType       string
	logger             Logger
}

func (r *runner) process() error {
	r.logger(Debug, "getting last checkpoint", nil)
	lastSequence, err := r.checkpoint.Get(r.checkpointIdentifier())
	if err != nil {
		r.logger(Error, "failed getting sequence number", LoggerData{"cause": fmt.Sprintf("%v", err)})
		return nil
	}

	r.logger(Debug, "getting shard iterator", nil)
	shardIterator, err := r.getShardIterator(lastSequence)
	if err != nil {
		r.logger(Error, "error getting shard iterator", LoggerData{"cause": fmt.Sprintf("%v", err)})
		r.Stop()
		return nil
	}

	// AWS documentation recommends GetRecords to be rate limited to 1 seconds to avoid
	// ProvisionedThroughputExceededExceptions.
	ticker := time.NewTicker(runnerTick)
	defer ticker.Stop() // nolint

	for range ticker.C {
		if r.Worker.State() == worker.StateStopping {
			return nil
		}

		r.logger(Debug, "getting records", nil)
		resp, err := r.client.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if ok && aerr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException {
				r.logger(Info, "the request rate for the stream is too high or the requested Data is too large for the available throughput, waiting...", nil)

				return nil
			}

			r.logger(Error, "error getting records", LoggerData{"cause": fmt.Sprintf("%v", err)})

			return nil
		}

		if resp.NextShardIterator == nil {
			r.logger(Info, "shard is closed, stopping runner", nil)
			r.Stop()

			return nil
		}

		if len(resp.Records) <= 0 {
			r.logger(Debug, "there is no records to process, jumping to next iteration", nil)
			shardIterator = resp.NextShardIterator

			continue
		}

		r.logger(Debug, "processing records", nil)
		for _, record := range resp.Records {
			if err := r.processRecord(record); err != nil {
				resp.NextShardIterator, err = r.getShardIterator(lastSequence)
				if err != nil {
					r.logger(Error, "error getting shard iterator", LoggerData{"cause": fmt.Sprintf("%v", err)})
					return nil
				}

				r.logger(Error, "error handling message", LoggerData{"cause": fmt.Sprintf("%v", err)})

				break
			}

			lastSequence = *record.SequenceNumber

			if r.checkpointStrategy == AfterRecord {
				r.logger(Debug, "setting checkpoint", nil)
				if err := r.checkpoint.Set(r.checkpointIdentifier(), lastSequence); err != nil {
					r.logger(Error, "error setting sequence number", LoggerData{"cause": fmt.Sprintf("%v", err)})

					return nil
				}
			}
		}

		if r.checkpointStrategy == AfterRecordBatch {
			if err := r.checkpoint.Set(r.checkpointIdentifier(), lastSequence); err != nil {
				r.logger(Error, "error setting sequence number", LoggerData{"cause": fmt.Sprintf("%v", err)})

				return nil
			}
		}

		shardIterator = resp.NextShardIterator
		r.logger(Debug, "records processed", nil)
	}

	return nil
}

func (r *runner) processRecord(record *kinesis.Record) error {
	message := Message{Partition: *record.PartitionKey, Data: record.Data}

	if err := r.handler(message); err != nil {
		return errors.Wrap(err, "error handling message")
	}

	return nil
}

func (r *runner) getShardIterator(sequence string) (*string, error) {
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

	iteratorOutput, err := r.client.GetShardIterator(getShardOptions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get shard iterator")
	}

	return iteratorOutput.ShardIterator, nil
}

func (r *runner) checkpointIdentifier() string {
	return fmt.Sprintf("%s_%s_%s", r.stream, r.group, r.shardID)
}

// Start starts runner.
func (r *runner) Start() error {
	notifier := worker.NewCronNotifier(r.tick)
	r.Worker = worker.NewWorker(r.process, worker.WithNotifier(notifier), worker.WithLogger(r.logger))

	return r.Worker.Start()
}
