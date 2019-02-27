package kinesis

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/kinesis"

	. "github.com/onsi/gomega"
)

func TestOptions_WithCheckpointStrategy_AfterRecord(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	options := ConsumerOptions{}

	// Act
	WithCheckpointStrategy(AfterRecord)(&options)

	// Assert
	Expect(options.checkpointStrategy).To(Equal(AfterRecord))
}

func TestOptions_WithClient(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	options := ConsumerOptions{}
	kinesisClient := &KinesisAPI{}

	// Act
	WithClient(kinesisClient)(&options)

	// Assert
	Expect(options.client).To(Equal(kinesisClient))
}

func TestOptions_SinceLatest(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	options := ConsumerOptions{}

	// Act
	SinceLatest()(&options)

	// Assert
	Expect(options.iteratorType).To(Equal(kinesis.ShardIteratorTypeLatest))
}

func TestOptions_SinceOldest(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	options := ConsumerOptions{}

	// Act
	SinceOldest()(&options)

	// Assert
	Expect(options.iteratorType).To(Equal(kinesis.ShardIteratorTypeTrimHorizon))
}

func TestOptions_SkipReshardingOrder(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	options := ConsumerOptions{}

	// Act
	SkipReshardingOrder()(&options)

	// Assert
	Expect(options.skipReshardingOrder).To(BeTrue())
}
