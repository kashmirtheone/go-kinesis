package kinesis

import (
	"context"
	"testing"

	"gitlab.com/marcoxavier/go-kinesis/mocks"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"

	. "github.com/onsi/gomega"
)

func TestStreamWatcher_CheckStream_Failing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	kinesisAPI := &mocks.KinesisAPI{}
	watcher := streamWatcher{
		client: kinesisAPI,
		stream: "some_stream",
	}
	describe := &kinesis.DescribeStreamInput{
		Limit:      aws.Int64(1),
		StreamName: aws.String(watcher.stream),
	}

	kinesisAPI.On("DescribeStreamWithContext", ctx, describe).Return(nil, errors.New("something failed"))

	// Act
	err := watcher.checkStream(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestStreamWatcher_CheckStream_NothingToDo(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	kinesisAPI := &mocks.KinesisAPI{}
	deleted := false
	deletingCallback := func() {
		deleted = true
	}
	watcher := streamWatcher{
		client:           kinesisAPI,
		stream:           "some_stream",
		deletingCallback: deletingCallback,
	}
	describe := &kinesis.DescribeStreamInput{
		Limit:      aws.Int64(1),
		StreamName: aws.String(watcher.stream),
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
	kinesisAPI := &mocks.KinesisAPI{}
	deleted := false
	deletingCallback := func() {
		deleted = true
	}
	watcher := streamWatcher{
		client:           kinesisAPI,
		stream:           "some_stream",
		deletingCallback: deletingCallback,
	}
	describe := &kinesis.DescribeStreamInput{
		Limit:      aws.Int64(1),
		StreamName: aws.String(watcher.stream),
	}
	response := &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{StreamStatus: aws.String(kinesis.StreamStatusDeleting)},
	}

	kinesisAPI.On("DescribeStreamWithContext", ctx, describe).Return(response, nil)

	// Act
	err := watcher.checkStream(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(deleted).To(BeTrue())
}
