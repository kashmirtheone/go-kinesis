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
	logger             Logger
	eventLogger        EventLogger
	closed             bool
	stopped            chan struct{}
}

// Start starts runner.
func (r *runner) Start(ctx context.Context) error {
	mCtx, cancel := context.WithCancel(ctx)
	r.shutdown = cancel
	defer close(r.stopped)

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

// Stop stops runner.
func (r *runner) Stop(ctx context.Context) error {
	r.shutdown()

	select {
	case <-r.stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("runner exit without ended stopping")
	}
}

// ShardID returns its shard id.
func (r *runner) ShardID() string {
	return r.shardID
}

// Closed return if runner's shard is closed.
func (r *runner) Closed() bool {
	return r.closed
}

func (r *runner) process(ctx context.Context) error {

	r.logger.Log(LevelDebug, nil, "getting last checkpoint")
	lastSequence, err := r.checkpoint.Get(r.checkpointIdentifier())
	if err != nil {
		r.logger.Log(LevelError, nil, "failed getting sequence number", loggerData{"cause": fmt.Sprintf("%v", err)})
		return nil
	}

	r.logger.Log(LevelDebug, nil, "getting shard iterator")
	shardIterator, err := r.getShardIterator(ctx, lastSequence)
	if err != nil {
		r.logger.Log(LevelError, loggerData{"cause": fmt.Sprintf("%v", err)}, "error getting shard iterator")
		return nil
	}

	// AWS documentation recommends GetRecords to be rate limited to 1 seconds to avoid
	// ProvisionedThroughputExceededExceptions.
	ticker := time.NewTicker(runnerTick)
	defer ticker.Stop() // nolint

	for {
		start := time.Now()

		r.logger.Log(LevelDebug, nil, "getting records")
		resp, err := r.client.GetRecordsWithContext(ctx, &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if ok && aerr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException {
				r.logger.Log(LevelInfo, nil, "the request rate for the stream is too high or the requested Data is too large for the available throughput, waiting...")

				return nil
			}

			r.logger.Log(LevelError, loggerData{"cause": fmt.Sprintf("%v", err)}, "error getting records")

			return nil
		}

		if resp.NextShardIterator == nil {
			r.logger.Log(LevelInfo, nil, "shard is closed, stopping runner")
			r.closed = true
			r.shutdown()

			return nil
		}

		if len(resp.Records) <= 0 {
			r.logger.Log(LevelDebug, nil, "there is no records to process, jumping to next iteration")
			shardIterator = resp.NextShardIterator

			goto next
		}

		r.logger.Log(LevelDebug, nil, "processing records")
		for _, record := range resp.Records {
			if err := r.processRecord(ctx, record); err != nil {
				resp.NextShardIterator, err = r.getShardIterator(ctx, lastSequence)
				if err != nil {
					r.logger.Log(LevelError, loggerData{"cause": fmt.Sprintf("%v", err)}, "error getting shard iterator")
					return nil
				}

				r.logger.Log(LevelError, loggerData{"cause": fmt.Sprintf("%v", err)}, "error handling message")

				break
			}

			lastSequence = aws.StringValue(record.SequenceNumber)

			if r.checkpointStrategy == AfterRecord {
				r.logger.Log(LevelDebug, nil, "setting checkpoint")
				if err := r.checkpoint.Set(r.checkpointIdentifier(), lastSequence); err != nil {
					r.logger.Log(LevelError, loggerData{"cause": fmt.Sprintf("%v", err)}, "error setting sequence number")

					return nil
				}
			}
		}

		if r.checkpointStrategy == AfterRecordBatch {
			if err := r.checkpoint.Set(r.checkpointIdentifier(), lastSequence); err != nil {
				r.logger.Log(LevelError, loggerData{"cause": fmt.Sprintf("%v", err)}, "error setting sequence number")

				return nil
			}
		}

		shardIterator = resp.NextShardIterator
		r.logger.Log(LevelDebug, nil, "records processed")

	next:
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.eventLogger.LogEvent(EventLog{Event: ShardIteratorTriggered, Elapse: time.Now().Sub(start)})
			continue
		}
	}
}

func (r *runner) processRecord(ctx context.Context, record *kinesis.Record) (err error) {
	start := time.Now()
	defer func() {
		if p := recover(); p != nil {
			err = errors.Wrap(fmt.Errorf("%s", p), "runner terminated due a panic")
		}

		if err != nil {
			r.eventLogger.LogEvent(EventLog{Event: RecordProcessedFail, Elapse: time.Now().Sub(start)})
		} else {
			r.eventLogger.LogEvent(EventLog{Event: RecordProcessedSuccess, Elapse: time.Now().Sub(start)})
		}
	}()

	message := Message{Partition: aws.StringValue(record.PartitionKey), Data: record.Data}

	if err := r.handler(ctx, message); err != nil {
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
