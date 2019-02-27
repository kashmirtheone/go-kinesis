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

// streamWatcher periodic checks stream status an notifies if is deleting.
type streamWatcher struct {
	stream           string
	tick             time.Duration
	client           kinesisiface.KinesisAPI
	deletingCallback func()
	logger           Logger
	eventLogger      EventLogger
}

// Run runs stream watcher.
func (s *streamWatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()

	for {
		if err := s.checkStream(ctx); err != nil {
			s.logger.Log(LevelError, loggerData{"cause": fmt.Sprintf("%v", err)}, "failed to check stream status")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			continue
		}
	}
}

// SetDeletingCallback callback is called when stream is deleting.
func (s *streamWatcher) SetDeletingCallback(cb func()) {
	s.deletingCallback = cb
}

func (s *streamWatcher) checkStream(ctx context.Context) error {
	start := time.Now()
	defer s.eventLogger.LogEvent(EventLog{Event: StreamCheckedTriggered, Elapse: time.Now().Sub(start)})

	s.logger.Log(LevelDebug, nil, "checking stream status")
	stream, err := s.client.DescribeStreamWithContext(ctx,
		&kinesis.DescribeStreamInput{
			Limit:      aws.Int64(1),
			StreamName: aws.String(s.stream),
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to describe stream")

	}

	if aws.StringValue(stream.StreamDescription.StreamStatus) == kinesis.StreamStatusDeleting {
		s.logger.Log(LevelInfo, nil, "stream is deleting")
		s.deletingCallback()
	}

	return nil
}
