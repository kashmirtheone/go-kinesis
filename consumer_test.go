package kinesis

import (
	"testing"

	"gitlab.com/marcoxavier/go-kinesis/mocks"

	. "github.com/onsi/gomega"
)

func TestConsumer_NewConsumer_InvalidConfiguration(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := mocks.Checkpoint{}
	config := ConsumerConfig{}
	handler := func(m Message) error { return nil }

	// Act
	client, err := NewConsumer(config, handler, &checkpoint)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(client).To(BeZero())
}

func TestConsumer_NewConsumer_InvalidHandler(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := mocks.Checkpoint{}
	config := ConsumerConfig{}

	// Act
	client, err := NewConsumer(config, nil, &checkpoint)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(client).To(BeZero())
}

func TestConsumer_NewConsumer_Success(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := mocks.Checkpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(m Message) error { return nil }

	// Act
	client, err := NewConsumer(config, handler, &checkpoint)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(client).ToNot(BeZero())
	Expect(client.checkpointStrategy).To(Equal(AfterRecordBatch))
	Expect(client.client).ToNot(BeNil())
}

func TestConsumer_NewConsumer_WithCheckpointStrategy_AfterRecord(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := mocks.Checkpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(m Message) error { return nil }

	// Act
	client, err := NewConsumer(config, handler, &checkpoint, WithCheckpointStrategy(AfterRecord))

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(client).ToNot(BeZero())
	Expect(client.checkpointStrategy).To(Equal(AfterRecord))
}

func TestConsumer_NewConsumer_WithClient(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := mocks.Checkpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(m Message) error { return nil }
	kinesisClient, _ := NewClient(config.AWS)

	// Act
	client, err := NewConsumer(config, handler, &checkpoint, WithClient(kinesisClient))

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(client).ToNot(BeZero())
	Expect(client.client).To(Equal(kinesisClient))
}
