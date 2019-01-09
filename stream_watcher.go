package kinesis

import (
	"fmt"
	"time"

	"gitlab.com/marcoxavier/go-kinesis/internal/worker"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

// streamWatcher periodic checks stream status an notifies if is deleting.
type streamWatcher struct {
	worker.Worker
	stream           string
	tick             time.Duration
	client           kinesisiface.KinesisAPI
	deletingCallback func()
	logger           Logger
}

func (s *streamWatcher) checkStream() error {
	s.logger(Debug, "checking stream status", nil)
	stream, err := s.client.DescribeStream(
		&kinesis.DescribeStreamInput{
			Limit:      aws.Int64(1),
			StreamName: aws.String(s.stream),
		},
	)

	if err != nil {
		s.logger(Error, "failed to check stream status", LoggerData{"cause": fmt.Sprintf("%v", err)})
		return nil
	}

	if *stream.StreamDescription.StreamStatus == kinesis.StreamStatusDeleting {
		s.logger(Info, "stream is deleting", nil)
		s.deletingCallback()
	}

	return nil
}

// Start starts watcher.
func (s *streamWatcher) Start() error {
	notifier := worker.NewCronNotifier(s.tick)
	s.Worker = worker.NewWorker(s.checkStream, worker.WithNotifier(notifier), worker.WithLogger(s.logger))

	return s.Worker.Start()
}
