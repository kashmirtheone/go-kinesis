package kinesis

import (
	"context"
	"github.com/golang/mock/gomock"
	"testing"

	"github.com/aws/aws-sdk-go/service/kinesis"

	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func TestProducer_NewProducer_InvalidConfiguration(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	config := ProducerConfig{}

	// Act
	producer, err := NewProducer(config)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(producer).To(BeZero())
}

func TestProducer_NewProducer_Success(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	config := ProducerConfig{
		Stream: "some_stream",
	}

	// Act
	producer, err := NewProducer(config)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(producer).ToNot(BeZero())
	Expect(producer.stream).To(Equal(config.Stream))
}

func TestProducer_PublishBatchWithContext_Failed(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	config := ProducerConfig{
		Stream: "some_stream",
	}
	messages := []Message{{PartitionKey: "some_partition", Data: []byte("some_data")}}
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	kinesisAPI.EXPECT().PutRecordsWithContext(ctx, gomock.Any()).Return(nil, errors.New("something failed"))

	// Act
	producer, _ := NewProducer(config)
	producer.client = kinesisAPI
	err := producer.PublishBatchWithContext(ctx, messages)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(producer).ToNot(BeZero())
}

func TestProducer_PublishBatchWithContext_Success(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	config := ProducerConfig{
		Stream: "some_stream",
	}
	messages := []Message{{PartitionKey: "some_partition", Data: []byte("some_data")}}
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	kinesisAPI.EXPECT().PutRecordsWithContext(ctx, gomock.Any()).Return(&kinesis.PutRecordsOutput{}, nil)

	// Act
	producer, _ := NewProducer(config)
	producer.client = kinesisAPI
	err := producer.PublishBatchWithContext(ctx, messages)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(producer).ToNot(BeZero())
}

func TestProducer_PublishWithContext_Failed(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	config := ProducerConfig{
		Stream: "some_stream",
	}
	message := Message{PartitionKey: "some_partition", Data: []byte("some_data")}
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	kinesisAPI.EXPECT().PutRecordWithContext(ctx, gomock.Any()).Return(nil, errors.New("something failed"))

	// Act
	producer, _ := NewProducer(config)
	producer.client = kinesisAPI
	err := producer.PublishWithContext(ctx, message)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(producer).ToNot(BeZero())
}

func TestProducer_PublishWithContext_Success(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	config := ProducerConfig{
		Stream: "some_stream",
	}
	message := Message{PartitionKey: "some_partition", Data: []byte("some_data")}
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	kinesisAPI.EXPECT().PutRecordWithContext(ctx, gomock.Any()).Return(&kinesis.PutRecordOutput{}, nil)

	// Act
	producer, _ := NewProducer(config)
	producer.client = kinesisAPI
	err := producer.PublishWithContext(ctx, message)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(producer).ToNot(BeZero())
}

func TestProducer_PublishBatch_Failed(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	config := ProducerConfig{
		Stream: "some_stream",
	}
	messages := []Message{{PartitionKey: "some_partition", Data: []byte("some_data")}}
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	kinesisAPI.EXPECT().PutRecords(gomock.Any()).Return(nil, errors.New("something failed"))

	// Act
	producer, _ := NewProducer(config)
	producer.client = kinesisAPI
	err := producer.PublishBatch(messages)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(producer).ToNot(BeZero())
}

func TestProducer_PublishBatch_Success(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	config := ProducerConfig{
		Stream: "some_stream",
	}
	messages := []Message{{PartitionKey: "some_partition", Data: []byte("some_data")}}
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	kinesisAPI.EXPECT().PutRecords(gomock.Any()).Return(&kinesis.PutRecordsOutput{}, nil)

	// Act
	producer, _ := NewProducer(config)
	producer.client = kinesisAPI
	err := producer.PublishBatch(messages)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(producer).ToNot(BeZero())
}

func TestProducer_Publish_Failed(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	config := ProducerConfig{
		Stream: "some_stream",
	}
	message := Message{PartitionKey: "some_partition", Data: []byte("some_data")}
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	kinesisAPI.EXPECT().PutRecord(gomock.Any()).Return(nil, errors.New("something failed"))

	// Act
	producer, _ := NewProducer(config)
	producer.client = kinesisAPI
	err := producer.Publish(message)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(producer).ToNot(BeZero())
}

func TestProducer_Publish_Success(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	config := ProducerConfig{
		Stream: "some_stream",
	}
	message := Message{PartitionKey: "some_partition", Data: []byte("some_data")}
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	kinesisAPI.EXPECT().PutRecord(gomock.Any()).Return(&kinesis.PutRecordOutput{}, nil)

	// Act
	producer, _ := NewProducer(config)
	producer.client = kinesisAPI
	err := producer.Publish(message)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(producer).ToNot(BeZero())
}
