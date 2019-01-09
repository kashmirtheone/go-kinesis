package kinesis

import (
	"testing"
	"time"

	"gitlab.com/marcoxavier/go-kinesis/mocks"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"

	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func TestRunner_Process_FailsToGetLastCheckpoint(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := &mocks.Checkpoint{}
	kinesisAPI := &mocks.KinesisAPI{}
	r := runner{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecordBatch,
		stream:             "some_stream",
		group:              "some_group",
		tick:               time.Hour,
		logger:             DumbLogger,
	}
	checkpoint.On("Get", r.checkpointIdentifier()).Return("", errors.New("something failed"))

	// Act
	err := r.process()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(checkpoint.AssertExpectations(t)).To(BeTrue(), "Should try to get last sequence")
}

func TestRunner_Process_FailsGettingShardIterator(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := &mocks.Checkpoint{}
	kinesisAPI := &mocks.KinesisAPI{}
	r := runner{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecordBatch,
		stream:             "some_stream",
		group:              "some_group",
		tick:               time.Hour,
		logger:             DumbLogger,
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	checkpoint.On("Get", r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.On("GetShardIterator", getShardIteratorInput).Return(nil, errors.New("something failed"))

	// Act
	err := r.process()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(kinesisAPI.AssertExpectations(t)).To(BeTrue(), "Should try to get shard iterator")
}

func TestRunner_Process_FailsGettingRecords(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := &mocks.Checkpoint{}
	kinesisAPI := &mocks.KinesisAPI{}
	r := runner{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecordBatch,
		stream:             "some_stream",
		group:              "some_group",
		tick:               time.Hour,
		logger:             DumbLogger,
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	checkpoint.On("Get", r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.On("GetShardIterator", getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.On("GetRecords", getRecordsInput).Return(nil, errors.New("something failed"))

	// Act
	err := r.process()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(kinesisAPI.AssertExpectations(t)).To(BeTrue(), "Should try to get records")
}

func TestRunner_Process_ShardClosedDoNothing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := &mocks.Checkpoint{}
	kinesisAPI := &mocks.KinesisAPI{}
	r := runner{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecordBatch,
		stream:             "some_stream",
		group:              "some_group",
		tick:               time.Hour,
		logger:             DumbLogger,
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: nil}
	checkpoint.On("Get", r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.On("GetShardIterator", getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.On("GetRecords", getRecordsInput).Return(getRecordsOutput, nil)

	// Act
	err := r.process()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(kinesisAPI.AssertExpectations(t)).To(BeTrue(), "Should try to get records")
}

func TestRunner_Process_FailsHandleRecord(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := &mocks.Checkpoint{}
	kinesisAPI := &mocks.KinesisAPI{}
	r := runner{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecordBatch,
		stream:             "some_stream",
		group:              "some_group",
		tick:               time.Hour,
		handler:            func(Message) error { return errors.New("something failed") },
		logger:             DumbLogger,
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	record := &kinesis.Record{PartitionKey: aws.String("some_partition"), Data: []byte("some_data"), SequenceNumber: aws.String("some_sequence_number2")}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("some_shard_iterator"), Records: []*kinesis.Record{record}}
	checkpoint.On("Get", r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.On("GetShardIterator", getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.On("GetRecords", getRecordsInput).Return(getRecordsOutput, nil)
	checkpoint.On("Set", r.checkpointIdentifier(), "some_sequence_number").Return(errors.New("something failed"))

	// Act
	err := r.process()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(kinesisAPI.AssertExpectations(t)).To(BeTrue(), "Should try to get records")
}

func TestRunner_Process_HandlesWithSuccess(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := &mocks.Checkpoint{}
	kinesisAPI := &mocks.KinesisAPI{}
	r := runner{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecordBatch,
		stream:             "some_stream",
		group:              "some_group",
		tick:               time.Hour,
		handler:            func(Message) error { return nil },
		logger:             DumbLogger,
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	record := &kinesis.Record{PartitionKey: aws.String("some_partition"), Data: []byte("some_data"), SequenceNumber: aws.String("some_sequence_number2")}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("some_shard_iterator"), Records: []*kinesis.Record{record}}
	checkpoint.On("Get", r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.On("GetShardIterator", getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.On("GetRecords", getRecordsInput).Return(getRecordsOutput, nil)
	checkpoint.On("Set", r.checkpointIdentifier(), "some_sequence_number2").Return(errors.New("something failed"))

	// Act
	err := r.process()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(kinesisAPI.AssertExpectations(t)).To(BeTrue(), "Should try to get records")
}

func TestRunner_Process_HandlesWithSuccessAfterRecordStrategy(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := &mocks.Checkpoint{}
	kinesisAPI := &mocks.KinesisAPI{}
	r := runner{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecord,
		stream:             "some_stream",
		group:              "some_group",
		tick:               time.Hour,
		handler:            func(Message) error { return nil },
		logger:             DumbLogger,
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	record := &kinesis.Record{PartitionKey: aws.String("some_partition"), Data: []byte("some_data"), SequenceNumber: aws.String("some_sequence_number2")}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("some_shard_iterator"), Records: []*kinesis.Record{record}}
	checkpoint.On("Get", r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.On("GetShardIterator", getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.On("GetRecords", getRecordsInput).Return(getRecordsOutput, nil)
	checkpoint.On("Set", r.checkpointIdentifier(), "some_sequence_number2").Return(errors.New("something failed"))

	// Act
	err := r.process()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(kinesisAPI.AssertExpectations(t)).To(BeTrue(), "Should try to get records")
}
