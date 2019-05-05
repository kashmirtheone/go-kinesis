package kinesis

import (
	"context"

	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
)

// Producer should be able to dispatch messages
type Producer struct {
	client kinesisiface.KinesisAPI
	stream string
}

// NewProducer creates a new kinesis producer
func NewProducer(config ProducerConfig) (*Producer, error) {
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid kinesis configuration")
	}

	client, err := NewClient(config.AWS)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a kinesis client")
	}

	return &Producer{
		client: client,
		stream: config.Stream,
	}, nil
}

// PublishWithContext publishes content to kinesis.
func (producer Producer) PublishWithContext(ctx context.Context, message Message) error {
	input := &kinesis.PutRecordInput{
		Data:         message.Data,
		PartitionKey: aws.String(message.PartitionKey),
		StreamName:   aws.String(producer.stream),
	}

	_, err := producer.client.PutRecordWithContext(ctx, input)
	if err != nil {
		return errors.Wrap(err, "error occurred publishing message")
	}

	return nil
}

// PublishBatchWithContext publishes contents to kinesis.
func (producer Producer) PublishBatchWithContext(ctx context.Context, messages []Message) error {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, len(messages))

	for _, m := range messages {
		record := &kinesis.PutRecordsRequestEntry{
			Data:         m.Data,
			PartitionKey: aws.String(m.PartitionKey),
		}

		records = append(records, record)
	}

	input := &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: aws.String(producer.stream),
	}

	_, err := producer.client.PutRecordsWithContext(ctx, input)
	if err != nil {
		return errors.Wrap(err, "error occurred publishing messages")
	}

	return nil
}

// Publish publishes content to kinesis.
func (producer Producer) Publish(message Message) error {
	input := &kinesis.PutRecordInput{
		Data:         message.Data,
		PartitionKey: aws.String(message.PartitionKey),
		StreamName:   aws.String(producer.stream),
	}

	_, err := producer.client.PutRecord(input)
	if err != nil {
		return errors.Wrap(err, "error occurred publishing message")
	}

	return nil
}

// PublishBatch publishes contents to kinesis.
func (producer Producer) PublishBatch(messages []Message) error {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, len(messages))

	for _, m := range messages {
		record := &kinesis.PutRecordsRequestEntry{
			Data:         m.Data,
			PartitionKey: aws.String(m.PartitionKey),
		}

		records = append(records, record)
	}

	input := &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: aws.String(producer.stream),
	}

	_, err := producer.client.PutRecords(input)
	if err != nil {
		return errors.Wrap(err, "error occurred publishing messages")
	}

	return nil
}
