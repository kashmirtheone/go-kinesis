package kinesis

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"

	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func TestRunner_Closed(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("", errors.New("something failed"))

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_FailsToGetLastCheckpoint(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("", errors.New("something failed"))

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_FailsGettingShardIterator(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(nil, errors.New("something failed"))

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_FailsGettingRecords(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(nil, errors.New("something failed"))

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_FailsGettingRecords_ErrCodeProvisionedThroughputExceededException(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	errorMock := NewMockError(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	errorMock.EXPECT().Code().Return(kinesis.ErrCodeProvisionedThroughputExceededException)
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(nil, errorMock)

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_FailsGettingRecords_ErrCodeExpiredIteratorException(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	errorMock := NewMockError(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	errorMock.EXPECT().Code().Return(kinesis.ErrCodeExpiredIteratorException)
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(nil, errorMock)

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_FailsGettingRecords_ErrCodeExpiredNextTokenException(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	errorMock := NewMockError(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	errorMock.EXPECT().Code().Return(kinesis.ErrCodeExpiredNextTokenException)
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(nil, errorMock)

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_ShardClosedDoNothing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	closed := false
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
		shutdown: func() {
			closed = true
		},
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: nil}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(getRecordsOutput, nil)

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(closed).To(BeTrue())
}

func TestRunner_Process_NoRecordsDoNothing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	nCtx, cancel := context.WithCancel(ctx)
	cancel()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	getRecordsOutput := &kinesis.GetRecordsOutput{Records: make([]*kinesis.Record, 0), NextShardIterator: aws.String("some_shard_iterator")}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(getRecordsOutput, nil)

	// Act
	err := r.process(nCtx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_FailsHandleRecord(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		handler:     func(_ context.Context, msg Message) error { return errors.New("something failed") },
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	record := &kinesis.Record{PartitionKey: aws.String("some_partition"), Data: []byte("some_data"), SequenceNumber: aws.String("some_sequence_number2")}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("some_shard_iterator"), Records: []*kinesis.Record{record}}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil).AnyTimes()
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(getRecordsOutput, nil)
	checkpoint.EXPECT().Set(r.checkpointIdentifier(), "some_sequence_number").Return(errors.New("something failed"))

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_FailsHandleRecordAndFailsGetShardIterator(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		handler:     func(_ context.Context, msg Message) error { return errors.New("something failed") },
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	record := &kinesis.Record{PartitionKey: aws.String("some_partition"), Data: []byte("some_data"), SequenceNumber: aws.String("some_sequence_number2")}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("some_shard_iterator"), Records: []*kinesis.Record{record}}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil).Times(1)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(getRecordsOutput, nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(nil, errors.New("something failed"))

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_PanicsHandleRecord(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
		handler: func(_ context.Context, msg Message) error {
			panic("something failed")
		},
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	record := &kinesis.Record{PartitionKey: aws.String("some_partition"), Data: []byte("some_data"), SequenceNumber: aws.String("some_sequence_number2")}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("some_shard_iterator"), Records: []*kinesis.Record{record}}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil).AnyTimes()
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(getRecordsOutput, nil)
	checkpoint.EXPECT().Set(r.checkpointIdentifier(), "some_sequence_number").Return(errors.New("something failed")).AnyTimes()

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_HandlesWithSuccessAfterBatchStrategyFails(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
		handler:     func(_ context.Context, msg Message) error { return nil },
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	record := &kinesis.Record{PartitionKey: aws.String("some_partition"), Data: []byte("some_data"), SequenceNumber: aws.String("some_sequence_number2")}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("some_shard_iterator"), Records: []*kinesis.Record{record}}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(getRecordsOutput, nil)
	checkpoint.EXPECT().Set(r.checkpointIdentifier(), "some_sequence_number2").Return(errors.New("something failed"))

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_HandlesWithSuccessAfterRecordStrategyFails(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
		handler:     func(_ context.Context, msg Message) error { return nil },
	}
	r.options.checkpointStrategy = AfterRecord
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	record := &kinesis.Record{PartitionKey: aws.String("some_partition"), Data: []byte("some_data"), SequenceNumber: aws.String("some_sequence_number2")}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("some_shard_iterator"), Records: []*kinesis.Record{record}}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(getRecordsOutput, nil)
	checkpoint.EXPECT().Set(r.checkpointIdentifier(), "some_sequence_number2").Return(errors.New("something failed"))

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Process_ProcessWithSuccess(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	r := runner{
		client:      kinesisAPI,
		checkpoint:  checkpoint,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		stopped:     make(chan struct{}),
		handler:     func(_ context.Context, msg Message) error { return nil },
	}
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(r.shardID),
		StreamName:             aws.String(r.config.Stream),
		ShardIteratorType:      aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StartingSequenceNumber: aws.String("some_sequence_number"),
	}
	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("some_shard_iterator")}
	getRecordsInput := &kinesis.GetRecordsInput{
		ShardIterator: getShardIteratorOutput.ShardIterator,
	}
	record := &kinesis.Record{PartitionKey: aws.String("some_partition"), Data: []byte("some_data"), SequenceNumber: aws.String("some_sequence_number2")}
	getRecordsOutput := &kinesis.GetRecordsOutput{NextShardIterator: aws.String("some_shard_iterator"), Records: []*kinesis.Record{record}}
	checkpoint.EXPECT().Get(r.checkpointIdentifier()).Return("some_sequence_number", nil)
	kinesisAPI.EXPECT().GetShardIterator(getShardIteratorInput).Return(getShardIteratorOutput, nil)
	kinesisAPI.EXPECT().GetRecords(getRecordsInput).Return(getRecordsOutput, nil)
	checkpoint.EXPECT().Set(r.checkpointIdentifier(), "some_sequence_number2").Return(nil).AnyTimes()

	// Act
	err := r.process(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Stop_WithTimeout(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Nanosecond)
	cancel()
	closed := false
	r := runner{
		stopped: make(chan struct{}),
		started: 1,
		shutdown: func() {
			closed = true
		},
	}

	// Act
	err := r.Stop(ctx)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(closed).To(BeTrue())
}

func TestRunner_Stop_Stopped_Runner(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	r := runner{
		stopped: make(chan struct{}),
	}

	// Act
	err := r.Stop(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunner_Stop_WithSuccess(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	closed := false
	r := runner{
		stopped: make(chan struct{}),
		started: 1,
		shutdown: func() {
			closed = true
		},
	}

	// Act
	close(r.stopped)
	err := r.Stop(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(closed).To(BeTrue())
}
