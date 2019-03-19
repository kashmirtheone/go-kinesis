package kinesis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	. "github.com/onsi/gomega"
)

func TestConsumer_NewConsumer_InvalidConfiguration(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := MockCheckpoint{}
	config := ConsumerConfig{}
	handler := func(_ context.Context, m Message) error { return nil }

	// Act
	client, err := NewConsumer(config, handler, &checkpoint)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(client).To(BeZero())
}

func TestConsumer_NewConsumer_InvalidHandler(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := MockCheckpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}

	// Act
	client, err := NewConsumer(config, nil, &checkpoint)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(client).To(BeZero())
}

func TestConsumer_NewConsumer_Success(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := MockCheckpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }

	// Act
	client, err := NewConsumer(config, handler, &checkpoint)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(client).ToNot(BeZero())
	Expect(client.checkpointStrategy).To(Equal(AfterRecordBatch))
	Expect(client.client).ToNot(BeNil())
}

func TestConsumer_Stats(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := MockCheckpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, err := NewConsumer(config, handler, &checkpoint)
	client.stats.RecordsFailed.Count = 1

	// Act
	stats := client.Stats()

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(stats).ToNot(BeZero())
}

func TestConsumer_Log(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := MockCheckpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, err := NewConsumer(config, handler, &checkpoint)
	var called bool
	log := &MockLogger{}
	log.On("Log", "some_level", mock.Anything, "some_format", mock.Anything).Run(func(args mock.Arguments) {
		called = true
	})

	// Act
	client.SetLogger(log)
	client.Log("some_level", nil, "some_format")

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(called).To(BeTrue())
}

func TestConsumer_LogEvent(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	checkpoint := MockCheckpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, err := NewConsumer(config, handler, &checkpoint)
	var called bool
	log := &MockEventLogger{}
	event := EventLog{Event: "some_event"}
	log.On("LogEvent", event).Run(func(args mock.Arguments) {
		called = true
	})

	// Act
	client.SetEventLogger(log)
	client.LogEvent(event)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(called).To(BeTrue())
}

func TestConsumer_Run_FailedToRunStreamWatcher(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	checkpoint := MockCheckpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, _ := NewConsumer(config, handler, &checkpoint)
	streamWatcher := &MockStreamChecker{}
	runnerFactory := &MockRunnerFactory{}
	client.streamWatcher = streamWatcher
	client.runnerFactory = runnerFactory

	streamWatcher.On("SetDeletingCallback", mock.Anything).Return()
	streamWatcher.On("Run", mock.Anything).Return(fmt.Errorf("some error"))
	runnerFactory.On("Run", mock.Anything).Return(nil)

	// Act
	err := client.Run(ctx)

	// Assert
	Expect(err).To(HaveOccurred())
}

func TestConsumer_Run_FailedToRunRunnerFactory(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	checkpoint := MockCheckpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, _ := NewConsumer(config, handler, &checkpoint)
	streamWatcher := &MockStreamChecker{}
	runnerFactory := &MockRunnerFactory{}
	client.streamWatcher = streamWatcher
	client.runnerFactory = runnerFactory

	streamWatcher.On("SetDeletingCallback", mock.Anything).Return()
	streamWatcher.On("Run", mock.Anything).Return(nil)
	runnerFactory.On("Run", mock.Anything).Return(fmt.Errorf("some error"))

	// Act
	err := client.Run(ctx)

	// Assert
	Expect(err).To(HaveOccurred())
}

func TestConsumer_Run_RunsWithSuccess(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	ctx := context.TODO()
	checkpoint := MockCheckpoint{}
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, _ := NewConsumer(config, handler, &checkpoint)
	streamWatcher := &MockStreamChecker{}
	runnerFactory := &MockRunnerFactory{}
	client.streamWatcher = streamWatcher
	client.runnerFactory = runnerFactory

	streamWatcher.On("SetDeletingCallback", mock.Anything).Return()
	streamWatcher.On("Run", mock.Anything).Return(nil)
	runnerFactory.On("Run", mock.Anything).Return(nil)

	// Act
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Millisecond)
	cancel()
	err := client.Run(ctxTimeout)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}
