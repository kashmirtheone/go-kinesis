package kinesis

import (
	"context"
	"testing"
	"time"

	"gitlab.com/marcoxavier/go-kinesis/mocks"

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
	kinesisAPI := &mocks.KinesisAPI{}
	factory := runnerFactory{
		client: kinesisAPI,
		stream: "some_stream",
		group:  "some_group",
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
	kinesisAPI := &mocks.KinesisAPI{}
	factory := runnerFactory{
		client: kinesisAPI,
		stream: "some_stream",
		group:  "some_group",
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

func TestRunnerFactory_CheckShards_CreateARunner(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	checkpoint := &mocks.Checkpoint{}
	kinesisAPI := &mocks.KinesisAPI{}
	factory := runnerFactory{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecord,
		stream:             "some_stream",
		group:              "some_group",
		runners:            make(map[string]*runner),
		runnerTick:         time.Hour,
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
	Expect(factory.runners["some_shard_id"].client).To(Equal(factory.client))
	Expect(factory.runners["some_shard_id"].checkpointStrategy).To(Equal(factory.checkpointStrategy))
	Expect(factory.runners["some_shard_id"].shardID).To(Equal("some_shard_id"))
	Expect(factory.runners["some_shard_id"].group).To(Equal(factory.group))
	Expect(factory.runners["some_shard_id"].stream).To(Equal(factory.stream))
}

func TestRunnerFactory_CheckShards_CreateARunnerWithSameShardID(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	checkpoint := &mocks.Checkpoint{}
	kinesisAPI := &mocks.KinesisAPI{}
	factory := runnerFactory{
		client:             kinesisAPI,
		checkpoint:         checkpoint,
		checkpointStrategy: AfterRecord,
		stream:             "some_stream",
		group:              "some_group",
		runners:            make(map[string]*runner),
		runnerTick:         time.Hour,
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
	Expect(factory.runners["some_shard_id"].client).To(Equal(factory.client))
	Expect(factory.runners["some_shard_id"].checkpointStrategy).To(Equal(factory.checkpointStrategy))
	Expect(factory.runners["some_shard_id"].shardID).To(Equal("some_shard_id"))
	Expect(factory.runners["some_shard_id"].group).To(Equal(factory.group))
	Expect(factory.runners["some_shard_id"].stream).To(Equal(factory.stream))
}
