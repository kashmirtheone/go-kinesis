package kinesis

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"

	. "github.com/onsi/gomega"
)

func TestStreamWatcher_CheckStream_Failing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	kinesisAPI := &KinesisAPI{}
	watcher := streamWatcher{
		client:      kinesisAPI,
		config:      config,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
	}
	describe := &kinesis.DescribeStreamInput{
		Limit:      aws.Int64(1),
		StreamName: aws.String(watcher.config.Stream),
	}

	kinesisAPI.On("DescribeStreamWithContext", ctx, describe).Return(nil, errors.New("something failed"))

	// Act
	err := watcher.checkStream(ctx)

	// Assert
	Expect(err).To(HaveOccurred())
}

func TestStreamWatcher_CheckStream_NothingToDo(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	kinesisAPI := &KinesisAPI{}
	deleted := false
	deletingCallback := func() {
		deleted = true
	}
	watcher := streamWatcher{
		client:           kinesisAPI,
		config:           config,
		deletingCallback: deletingCallback,
		logger:           &dumbLogger{},
		eventLogger:      &dumbEventLogger{},
	}
	describe := &kinesis.DescribeStreamInput{
		Limit:      aws.Int64(1),
		StreamName: aws.String(watcher.config.Stream),
	}
	response := &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{StreamStatus: aws.String("some_status")},
	}

	kinesisAPI.On("DescribeStreamWithContext", ctx, describe).Return(response, nil)

	// Act
	err := watcher.checkStream(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(deleted).To(BeFalse())
}

func TestStreamWatcher_CheckStream_DeletingStream(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	kinesisAPI := &KinesisAPI{}
	deleted := false
	deletingCallback := func() {
		deleted = true
	}
	watcher := streamWatcher{
		client:      kinesisAPI,
		config:      config,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
	}
	describe := &kinesis.DescribeStreamInput{
		Limit:      aws.Int64(1),
		StreamName: aws.String(watcher.config.Stream),
	}
	response := &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{StreamStatus: aws.String(kinesis.StreamStatusDeleting)},
	}

	kinesisAPI.On("DescribeStreamWithContext", ctx, describe).Return(response, nil)

	// Act
	watcher.SetDeletingCallback(deletingCallback)
	err := watcher.checkStream(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(deleted).To(BeTrue())
}

func TestStreamWatcher_Run_ReturnsError(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	kinesisAPI := &KinesisAPI{}
	mockLogger := &MockLogger{}
	watcher := streamWatcher{
		client:      kinesisAPI,
		config:      config,
		logger:      mockLogger,
		eventLogger: &dumbEventLogger{},
	}
	describe := &kinesis.DescribeStreamInput{
		Limit:      aws.Int64(1),
		StreamName: aws.String(watcher.config.Stream),
	}
	called := false
	kinesisAPI.On("DescribeStreamWithContext", mock.Anything, describe).Return(nil, errors.New("something failed"))
	mockLogger.On("Log", LevelError, mock.Anything, mock.Anything, mock.Anything).Run(func(_ mock.Arguments) {
		called = true
	})
	mockLogger.On("Log", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()

	// Act
	canceledTimeout, cancel := context.WithCancel(ctx)
	cancel()
	err := watcher.Run(canceledTimeout)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(called).To(BeTrue())
}
