package kinesis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	. "github.com/onsi/gomega"
)

var (
	config = ConsumerConfig{
		Stream:               "some_stream",
		Group:                "some_group",
		RunnerTick:           time.Hour,
		StreamCheckTick:      time.Nanosecond,
		RunnerGetRecordsRate: time.Second,
	}
	options = ConsumerOptions{
		checkpointStrategy: AfterRecordBatch,
		iteratorType:       IteratorTypeHead,
	}
)

func TestConsumer_NewConsumer_InvalidConfiguration(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{}
	handler := func(_ context.Context, m Message) error { return nil }

	// Act
	client, err := NewConsumer(config, handler, checkpoint)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(client).To(BeZero())
}

func TestConsumer_NewConsumer_InvalidHandler(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}

	// Act
	client, err := NewConsumer(config, nil, checkpoint)

	// Assert
	Expect(err).To(HaveOccurred())
	Expect(client).To(BeZero())
}

func TestConsumer_NewConsumer_Success(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }

	// Act
	client, err := NewConsumer(config, handler, checkpoint)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(client).ToNot(BeZero())
	Expect(client.checkpointStrategy).To(Equal(AfterRecordBatch))
	Expect(client.client).ToNot(BeNil())
}

func TestConsumer_Stats(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, err := NewConsumer(config, handler, checkpoint)
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
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, err := NewConsumer(config, handler, checkpoint)
	var called bool
	log := NewMockLogger(mockCtrl)
	log.EXPECT().Log("some_level", gomock.Any(), "some_format", gomock.Any()).DoAndReturn(func(arg0, arg1, arg2 interface{}, arg3 ...interface{}) {
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
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, err := NewConsumer(config, handler, checkpoint)
	var called bool
	log := NewMockEventLogger(mockCtrl)
	event := EventLog{Event: "some_event"}
	log.EXPECT().LogEvent(event).DoAndReturn(func(interface{}) {
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
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, _ := NewConsumer(config, handler, checkpoint)
	streamWatcher := NewMockStreamChecker(mockCtrl)
	runnerFactory := NewMockRunnerFactory(mockCtrl)
	client.streamWatcher = streamWatcher
	client.runnerFactory = runnerFactory

	streamWatcher.EXPECT().SetDeletingCallback(gomock.Any()).Return()
	streamWatcher.EXPECT().Run(gomock.Any()).Return(fmt.Errorf("some error"))
	runnerFactory.EXPECT().Run(gomock.Any()).Return(nil)

	// Act
	err := client.Run(ctx)

	// Assert
	Expect(err).To(HaveOccurred())
}

func TestConsumer_Run_FailedToRunRunnerFactory(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, _ := NewConsumer(config, handler, checkpoint)
	streamWatcher := NewMockStreamChecker(mockCtrl)
	runnerFactory := NewMockRunnerFactory(mockCtrl)
	client.streamWatcher = streamWatcher
	client.runnerFactory = runnerFactory

	streamWatcher.EXPECT().SetDeletingCallback(gomock.Any()).Return()
	streamWatcher.EXPECT().Run(gomock.Any()).Return(nil)
	runnerFactory.EXPECT().Run(gomock.Any()).Return(fmt.Errorf("some error"))

	// Act
	err := client.Run(ctx)

	// Assert
	Expect(err).To(HaveOccurred())
}

func TestConsumer_Run_RunsWithSuccess(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ctx := context.TODO()
	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, _ := NewConsumer(config, handler, checkpoint)
	streamWatcher := NewMockStreamChecker(mockCtrl)
	runnerFactory := NewMockRunnerFactory(mockCtrl)
	client.streamWatcher = streamWatcher
	client.runnerFactory = runnerFactory

	streamWatcher.EXPECT().SetDeletingCallback(gomock.Any()).Return()
	streamWatcher.EXPECT().Run(gomock.Any()).Return(nil)
	runnerFactory.EXPECT().Run(gomock.Any()).Return(nil)

	// Act
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Millisecond)
	cancel()
	err := client.Run(ctxTimeout)

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

/*
func TestConsumer_StartStop_WithSuccess(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	checkpoint := NewMockCheckpoint(mockCtrl)
	config := ConsumerConfig{
		AWS:    AWSConfig{},
		Stream: "some_stream",
		Group:  "some_group",
	}
	handler := func(_ context.Context, m Message) error { return nil }
	client, _ := NewConsumer(config, handler, checkpoint)
	streamWatcher := NewMockStreamChecker(mockCtrl)
	runnerFactory := NewMockRunnerFactory(mockCtrl)
	client.streamWatcher = streamWatcher
	client.runnerFactory = runnerFactory

	streamWatcher.EXPECT().SetDeletingCallback(gomock.Any()).Return()
	streamWatcher.EXPECT().Run(gomock.Any()).Return(nil)
	runnerFactory.EXPECT().Run(gomock.Any()).Return(nil)

	// Act
	go func() {
		time.Sleep(time.Millisecond)
		serr := client.Stop()
		Expect(serr).ToNot(HaveOccurred())
	}()
	err := client.Start()

	// Assert
	Expect(err).ToNot(HaveOccurred())
}
*/
