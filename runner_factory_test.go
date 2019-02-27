package kinesis

import (
	"context"
	"testing"
	"time"

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
		stream:      "some_stream",
		group:       "some_group",
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
	}
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.stream),
	}
	kinesisAPI.On("ListShardsWithContext", ctx, input).Return(nil, errors.New("something failed"))

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
		stream:      "some_stream",
		group:       "some_group",
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
	}
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.stream),
	}
	output := &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{}}
	kinesisAPI.On("ListShardsWithContext", ctx, input).Return(output, nil)

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
		stream:      "some_stream",
		group:       "some_group",
		runners:     make(map[string]Runner),
		logger:      &dumbLogger{},
		eventLogger: &dumbEventLogger{},
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id"}
	factory.runners[parent.shardID] = parent
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.stream),
	}
	output := &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID)}}}
	kinesisAPI.On("ListShardsWithContext", ctx, input).Return(output, nil)

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
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecord,
		stream:             "some_stream",
		group:              "some_group",
		runners:            make(map[string]Runner),
		runnerTick:         time.Hour,
		logger:             &dumbLogger{},
		eventLogger:        &dumbEventLogger{},
	}
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.stream),
	}
	shard := &kinesis.Shard{
		ShardId: aws.String("some_shard_id"),
	}
	output := &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{shard}}
	kinesisAPI.On("ListShardsWithContext", ctx, input).Return(output, nil)
	checkpoint.On("Get", mock.Anything).Return("", errors.New("something failed"))

	// Act
	err := factory.checkShards(ctx)
	//factory.Stop()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(len(factory.runners)).To(Equal(1))
	Expect(factory.runners["some_shard_id"]).ToNot(BeNil())
}

func TestRunnerFactory_CheckShards_CreateARunnerWithSameShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	checkpoint := &MockCheckpoint{}
	kinesisAPI := &KinesisAPI{}
	factory := runnerFactory{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecord,
		stream:             "some_stream",
		group:              "some_group",
		runners:            make(map[string]Runner),
		runnerTick:         time.Hour,
		logger:             &dumbLogger{},
		eventLogger:        &dumbEventLogger{},
	}
	input := &kinesis.ListShardsInput{
		StreamName: aws.String(factory.stream),
	}
	shard1 := &kinesis.Shard{
		ShardId: aws.String("some_shard_id"),
	}
	shard2 := &kinesis.Shard{
		ShardId: aws.String("some_shard_id"),
	}
	output := &kinesis.ListShardsOutput{Shards: []*kinesis.Shard{shard1, shard2}}
	kinesisAPI.On("ListShardsWithContext", ctx, input).Return(output, nil)
	checkpoint.On("Get", mock.Anything).Return("", errors.New("something failed"))

	// Act
	err := factory.checkShards(ctx)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(len(factory.runners)).To(Equal(1))
	Expect(factory.runners["some_shard_id"]).ToNot(BeNil())
}

func TestRunnerFactory_ShouldStart_WithSkipReshardingOrder(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners:             make(map[string]Runner),
		skipReshardingOrder: true,
	}
	r := &runner{shardID: "some_shard_id"}
	factory.runners[r.shardID] = r
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	Expect(len(factory.runners)).To(Equal(1))
	Expect(factory.runners["some_shard_id"]).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithoutShardParentID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: make(map[string]Runner),
	}
	r := &runner{shardID: "some_shard_id"}
	factory.runners[r.shardID] = r
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	Expect(len(factory.runners)).To(Equal(1))
	Expect(factory.runners["some_shard_id"]).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithUnexistingParentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: make(map[string]Runner),
	}
	r := &runner{shardID: "some_shard_id"}
	factory.runners[r.shardID] = r
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String("unexisting_parent_shard_id")}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	Expect(len(factory.runners)).To(Equal(1))
	Expect(factory.runners["some_shard_id"]).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithClosedParentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: make(map[string]Runner),
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	factory.runners[r.shardID] = r
	factory.runners[parent.shardID] = parent
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	Expect(len(factory.runners)).To(Equal(2))
	Expect(factory.runners[r.shardID]).ToNot(BeNil())
	Expect(factory.runners[parent.shardID]).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithNotClosedParentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: make(map[string]Runner),
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id"}
	factory.runners[r.shardID] = r
	factory.runners[parent.shardID] = parent
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	Expect(len(factory.runners)).To(Equal(2))
	Expect(factory.runners[r.shardID]).ToNot(BeNil())
	Expect(factory.runners[parent.shardID]).ToNot(BeNil())
	Expect(shouldStart).To(BeFalse())
}

func TestRunnerFactory_ShouldStart_WithoutShardAdjacentID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: make(map[string]Runner),
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	factory.runners[r.shardID] = r
	factory.runners[parent.shardID] = parent
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	Expect(len(factory.runners)).To(Equal(2))
	Expect(factory.runners[r.shardID]).ToNot(BeNil())
	Expect(factory.runners[parent.shardID]).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithUnexistingAdjacentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: make(map[string]Runner),
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	factory.runners[r.shardID] = r
	factory.runners[parent.shardID] = parent
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID), AdjacentParentShardId: aws.String("nonexistent_adjacent_shard_id")}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	Expect(len(factory.runners)).To(Equal(2))
	Expect(factory.runners[r.shardID]).ToNot(BeNil())
	Expect(factory.runners[parent.shardID]).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithClosedAdjacentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: make(map[string]Runner),
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	adjacent := &runner{shardID: "adjacent_shard_id", closed: true}
	factory.runners[r.shardID] = r
	factory.runners[parent.shardID] = parent
	factory.runners[adjacent.shardID] = adjacent
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID), AdjacentParentShardId: aws.String(adjacent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	Expect(len(factory.runners)).To(Equal(3))
	Expect(factory.runners[r.shardID]).ToNot(BeNil())
	Expect(factory.runners[parent.shardID]).ToNot(BeNil())
	Expect(factory.runners[adjacent.shardID]).ToNot(BeNil())
	Expect(shouldStart).To(BeTrue())
}

func TestRunnerFactory_ShouldStart_WithNotClosedAdjacentShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	factory := runnerFactory{
		runners: make(map[string]Runner),
	}
	r := &runner{shardID: "some_shard_id"}
	parent := &runner{shardID: "parent_shard_id", closed: true}
	adjacent := &runner{shardID: "adjacent_shard_id"}
	factory.runners[r.shardID] = r
	factory.runners[parent.shardID] = parent
	factory.runners[adjacent.shardID] = adjacent
	shard := &kinesis.Shard{ShardId: aws.String(r.shardID), ParentShardId: aws.String(parent.shardID), AdjacentParentShardId: aws.String(adjacent.shardID)}

	// Act
	shouldStart := factory.shouldStart(shard)

	// Assert
	Expect(len(factory.runners)).To(Equal(3))
	Expect(factory.runners[r.shardID]).ToNot(BeNil())
	Expect(factory.runners[parent.shardID]).ToNot(BeNil())
	Expect(factory.runners[adjacent.shardID]).ToNot(BeNil())
	Expect(shouldStart).To(BeFalse())
}
