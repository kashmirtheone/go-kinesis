package kinesis

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"

	. "github.com/onsi/gomega"
)

func TestRunnerFactory_CheckShards_Failing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	kinesisAPI := &KinesisAPI{}
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
	kinesisAPI.On("ListShards", input).Return(nil, errors.New("something failed"))

	// Act
	err := factory.checkShards(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunnerFactory_CheckShards_DoNothing(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	kinesisAPI := &KinesisAPI{}
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
	kinesisAPI.On("ListShards", input).Return(output, nil)

	// Act
	err := factory.checkShards(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunnerFactory_CheckShards_ShouldNotCreateRunner(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	kinesisAPI := &KinesisAPI{}
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
	kinesisAPI.On("ListShards", input).Return(output, nil)

	// Act
	err := factory.checkShards(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestRunnerFactory_CheckShards_CreateARunner(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	checkpoint := &MockCheckpoint{}
	kinesisAPI := &KinesisAPI{}
	factory := runnerFactory{
		client:      kinesisAPI,
		config:      config,
		options:     options,
		checkpoint:  checkpoint,
		runners:     sync.Map{},
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
	}
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.config.Stream),
	}
	shard := &kinesis.Shard{
		ShardId: aws.String("some_shard_id"),
	}
	output := &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{shard}}
	kinesisAPI.On("ListShards", input).Return(output, nil)
	checkpoint.On("Get", mock.Anything).Return("", errors.New("something failed"))

	// Act
	err := factory.checkShards(ctx)
	//factory.Stop()

	// Assert
	runner, _ := factory.runners.Load("some_shard_id")
	Expect(err).ToNot(HaveOccurred())
	Expect(runner).ToNot(BeNil())
}

func TestRunnerFactory_CheckShards_CreateARunnerWithSameShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	checkpoint := &MockCheckpoint{}
	kinesisAPI := &KinesisAPI{}
	factory := runnerFactory{
		client:      kinesisAPI,
		options:     options,
		config:      config,
		checkpoint:  checkpoint,
		runners:     sync.Map{},
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
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
	kinesisAPI.On("ListShards", input).Return(output, nil)
	checkpoint.On("Get", mock.Anything).Return("", errors.New("something failed"))

	// Act
	err := factory.checkShards(ctx)

	// Assert
	runner, _ := factory.runners.Load("some_shard_id")
	Expect(err).ToNot(HaveOccurred())
	Expect(runner).ToNot(BeNil())
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
