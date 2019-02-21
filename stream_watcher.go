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
}

// Run runs stream watcher.
func (s *streamWatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()

	for {
		if err := s.checkStream(ctx); err != nil {
			return errors.Wrap(err, "failed to check stream")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			continue
		}
	}
}

func (s *streamWatcher) checkStream(ctx context.Context) error {
	log(Debug, nil, "checking stream status")
	stream, err := s.client.DescribeStreamWithContext(ctx,
		&kinesis.DescribeStreamInput{
			Limit:      aws.Int64(1),
			StreamName: aws.String(s.stream),
		},
	)

	if err != nil {
		log(Error, loggerData{"cause": fmt.Sprintf("%v", err)}, "failed to check stream status")
		return nil

	}

	if *stream.StreamDescription.StreamStatus == kinesis.StreamStatusDeleting {
		log(Info, nil, "stream is deleting")
		s.deletingCallback()
	}

	return nil
}
