package kinesis

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"

	. "github.com/onsi/gomega"
)

func TestRunnerFactory_CheckShards_Failing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	factory := runnerFactory{
		client:      kinesisAPI,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
	}
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.config.Stream),
	}
	kinesisAPI.EXPECT().ListShards(input).Return(nil, errors.New("something failed"))

	// Act
	err := factory.checkShards(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunnerFactory_CheckShards_DoNothing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	factory := runnerFactory{
		client:      kinesisAPI,
		config:      config,
		options:     options,
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
	}
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.config.Stream),
	}
	output := &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{}}
	kinesisAPI.EXPECT().ListShards(input).Return(output, nil)

	// Act
	err := factory.checkShards(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunnerFactory_CheckShards_ShouldNotCreateRunner(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	factory := runnerFactory{
		client:      kinesisAPI,
		config:      config,
		options:     options,
		runners:     sync.Map{},
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id"}
	factory.runners.Store(parent.shardID, parent)
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.config.Stream),
	}
	output := &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID)}}}
	kinesisAPI.EXPECT().ListShards(input).Return(output, nil)

	// Act
	err := factory.checkShards(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunnerFactory_CheckShards_CreateARunner(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	runner := NewMockRunner(mockCtrl)
	factory := runnerFactory{
		client:      kinesisAPI,
		config:      config,
		options:     options,
		checkpoint:  checkpoint,
		runners:     sync.Map{},
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		factory: func(shardID string, iteratorConfig ConsumerIterator) Runner {
			return runner
		},
	}
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.config.Stream),
	}
	shard := &kinesis.Shard{
		ShardId: aws.String("some_shard_id"),
	}
	output := &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{shard}}
	var started int32
	kinesisAPI.EXPECT().ListShards(input).Return(output, nil)
	runner.EXPECT().ShardID().Return("some_shard_id").Times(1)
	runner.EXPECT().Start(gomock.Any()).DoAndReturn(func(...interface{}) error {
		atomic.SwapInt32(&started, 1)
		return nil
	}).Times(1)

	// Act
	err := factory.checkShards(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Eventually(func() bool { return atomic.LoadInt32(&started) == 1 }, time.Second).Should(BeTrue())
}

func TestRunnerFactory_CheckShards_CreateARunnerWithSameShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	kinesisAPI := NewMockKinesisAPI(mockCtrl)
	runner := NewMockRunner(mockCtrl)
	factory := runnerFactory{
		client:      kinesisAPI,
		options:     options,
		config:      config,
		checkpoint:  checkpoint,
		runners:     sync.Map{},
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
		factory: func(shardID string, iteratorConfig ConsumerIterator) Runner {
			return runner
		},
	}
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.config.Stream),
	}
	shard1 := &kinesis.Shard{
		ShardId: aws.String("some_shard_id"),
	}
	shard2 := &kinesis.Shard{
		ShardId: aws.String("some_shard_id"),
	}
	output := &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{shard1, shard2}}
	var started int32
	kinesisAPI.EXPECT().ListShards(input).Return(output, nil)
	runner.EXPECT().ShardID().Return("some_shard_id").Times(1)
	runner.EXPECT().Start(gomock.Any()).DoAndReturn(func(...interface{}) error {
		atomic.SwapInt32(&started, 1)
		return nil
	}).Times(1)

	// Act
	err := factory.checkShards(ctx)
	time.Sleep(time.Second)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Eventually(func() bool { return atomic.LoadInt32(&started) == 1 }, time.Second).Should(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithSkipReshardingOrder(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: sync.Map{},
		config:  config,
		options: options,
	}
	factory.options.skipReshardingOrder = true
	r := &runner{shardID: "some_shard_id"}
	factory.runners.Store(r.shardID, r)
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	runner, _ := factory.runners.Load("some_shard_id")
	Expect(runner).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithoutShardParentID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	factory := runnerFactory{
		runners: sync.Map{},
		config:  config,
		options: options,
	}
	r := &runner{shardID: "some_shard_id"}
	factory.runners.Store(r.shardID, r)
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	runner, _ := factory.runners.Load("some_shard_id")
	Expect(runner).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithUnexistingParentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	factory := runnerFactory{
		runners: sync.Map{},
		config:  config,
		options: options,
	}
	r := &runner{shardID: "some_shard_id"}
	factory.runners.Store(r.shardID, r)
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String("unexisting_parent_shard_id")}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	runner, _ := factory.runners.Load("some_shard_id")
	Expect(runner).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithClosedParentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: sync.Map{},
		config:  config,
		options: options,
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	factory.runners.Store(r.shardID, r)
	factory.runners.Store(parent.shardID, parent)
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	runner, _ := factory.runners.Load(r.shardID)
	parentRunner, _ := factory.runners.Load(parent.shardID)
	Expect(runner).ToNot(BeNil())
	Expect(parentRunner).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithNotClosedParentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	factory := runnerFactory{
		runners: sync.Map{},
		config:  config,
		options: options,
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id"}
	factory.runners.Store(r.shardID, r)
	factory.runners.Store(parent.shardID, parent)
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	runner, _ := factory.runners.Load(r.shardID)
	parentRunner, _ := factory.runners.Load(parent.shardID)
	Expect(runner).ToNot(BeNil())
	Expect(parentRunner).ToNot(BeNil())
	Expect(shouldStart).To(BeFalse())
}

func TestRunnerFactory_ShouldStart_WithoutShardAdjacentID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: sync.Map{},
		config:  config,
		options: options,
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	factory.runners.Store(r.shardID, r)
	factory.runners.Store(parent.shardID, parent)
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	runner, _ := factory.runners.Load(r.shardID)
	parentRunner, _ := factory.runners.Load(parent.shardID)
	Expect(runner).ToNot(BeNil())
	Expect(parentRunner).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithUnexistingAdjacentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	factory := runnerFactory{
		runners: sync.Map{},
		config:  config,
		options: options,
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	factory.runners.Store(r.shardID, r)
	factory.runners.Store(parent.shardID, parent)
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID), AdjacentParentShardId: aws.String("nonexistent_adjacent_shard_id")}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	runner, _ := factory.runners.Load(r.shardID)
	parentRunner, _ := factory.runners.Load(parent.shardID)
	Expect(runner).ToNot(BeNil())
	Expect(parentRunner).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithClosedAdjacentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	factory := runnerFactory{
		runners: sync.Map{},
		config:  config,
		options: options,
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	adjacent := &runner{shardID: "adjacent_shard_id", closed: true}
	factory.runners.Store(r.shardID, r)
	factory.runners.Store(parent.shardID, parent)
	factory.runners.Store(adjacent.shardID, adjacent)
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID), AdjacentParentShardId: aws.String(adjacent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	runner, _ := factory.runners.Load(r.shardID)
	parentRunner, _ := factory.runners.Load(parent.shardID)
	adjacentRunner, _ := factory.runners.Load(adjacent.shardID)
	Expect(runner).ToNot(BeNil())
	Expect(parentRunner).ToNot(BeNil())
	Expect(adjacentRunner).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithNotClosedAdjacentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	factory := runnerFactory{
		runners: sync.Map{},
		config:  config,
		options: options,
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	adjacent := &runner{shardID: "adjacent_shard_id"}
	factory.runners.Store(r.shardID, r)
	factory.runners.Store(parent.shardID, parent)
	factory.runners.Store(adjacent.shardID, adjacent)
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID), AdjacentParentShardId: aws.String(adjacent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	runner, _ := factory.runners.Load(r.shardID)
	parentRunner, _ := factory.runners.Load(parent.shardID)
	adjacentRunner, _ := factory.runners.Load(adjacent.shardID)
	Expect(runner).ToNot(BeNil())
	Expect(parentRunner).ToNot(BeNil())
	Expect(adjacentRunner).ToNot(BeNil())
	Expect(shouldStart).To(BeFalse())
}
