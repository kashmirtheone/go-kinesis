package kinesis

import (
	"testing"

	"github.com/golang/mock/gomock"

	. "github.com/onsi/gomega"
)

func TestOptions_WithCheckpointStrategy_AfterRecord(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	options := ConsumerOptions{}

	// Act
	WithCheckpointStrategy(AfterRecord)(&options)

	// Assert
	Expect(options.checkpointStrategy).To(Equal(AfterRecord))
}

func TestOptions_WithClient(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	options := ConsumerOptions{}
	kinesisClient := &MockKinesisAPI{}

	// Act
	WithClient(kinesisClient)(&options)

	// Assert
	Expect(options.client).To(Equal(kinesisClient))
}

func TestOptions_WithSpecificIterators(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	options := ConsumerOptions{}
	iterator := ConsumerIterator{Type: IteratorTypeHead, ShardID: "some_id", Sequence: "some_sequence"}

	// Act
	WithSpecificIterators(map[string]ConsumerIterator{iterator.ShardID: iterator})(&options)

	// Assert
	Expect(options.iterators["some_id"]).To(Equal(iterator))
}

func TestOptions_WithSpecificShards(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	options := ConsumerOptions{}

	// Act
	WithShards("some_id")(&options)

	// Assert
	Expect(options.specificShards["some_id"]).ToNot(BeNil())
}

func TestOptions_SinceLatest(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	options := ConsumerOptions{}

	// Act
	SinceLatest()(&options)

	// Assert
	Expect(options.iteratorType).To(Equal(IteratorTypeTail))
}

func TestOptions_SinceOldest(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	options := ConsumerOptions{}

	// Act
	SinceOldest()(&options)

	// Assert
	Expect(options.iteratorType).To(Equal(IteratorTypeHead))
}

func TestOptions_SinceSequence(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	options := ConsumerOptions{
		iterators: make(map[string]ConsumerIterator),
	}

	// Act
	SinceSequence("some_shard", "some_sequence")(&options)

	// Assert
	Expect(len(options.iterators)).To(Equal(1))
	Expect(options.iterators["some_shard"].Type).To(Equal(IteratorTypeSequence))
	Expect(options.iterators["some_shard"].ShardID).To(Equal("some_shard"))
	Expect(options.iterators["some_shard"].Sequence).To(Equal("some_sequence"))
}

func TestOptions_SkipReshardingOrder(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	options := ConsumerOptions{}

	// Act
	SkipReshardingOrder()(&options)

	// Assert
	Expect(options.skipReshardingOrder).To(BeTrue())
}
