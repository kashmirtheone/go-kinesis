package kinesis

import (
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"

	. "github.com/onsi/gomega"
)

func TestStreamWatcher_CheckStream_Failing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	kinesisAPI := NewMockKinesisAPI(mockCtrl)
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

	kinesisAPI.EXPECT().DescribeStream(describe).Return(nil, errors.New("something failed"))

	// Act
	err := watcher.checkStream()

	// Assert
	Expect(err).To(HaveOccurred())
}

func TestStreamWatcher_CheckStream_NothingToDo(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	kinesisAPI := NewMockKinesisAPI(mockCtrl)
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

	kinesisAPI.EXPECT().DescribeStream(describe).Return(response, nil)

	// Act
	err := watcher.checkStream()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(deleted).To(BeFalse())
}

func TestStreamWatcher_CheckStream_DeletingStream(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	kinesisAPI := NewMockKinesisAPI(mockCtrl)
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

	kinesisAPI.EXPECT().DescribeStream(describe).Return(response, nil)

	// Act
	watcher.SetDeletingCallback(deletingCallback)
	err := watcher.checkStream()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(deleted).To(BeTrue())
}
