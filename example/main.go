package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	logger "gitlab.com/vredens/go-logger"

	kinesis "github.com/kashmirtheone/go-kinesis"
	"github.com/kashmirtheone/go-kinesis/checkpoint/memory"
)

var log = logger.Spawn(logger.WithTags("kinesis-consumer"))

func handler(_ context.Context, message kinesis.Message) error {
	fmt.Printf("partition: %s, data: %s\n", message.Partition, string(message.Data))
	return nil
}

// Logger ...
type Logger struct {
}

// Log logs kinesis consumer.
func (l *Logger) Log(level string, data map[string]interface{}, format string, args ...interface{}) {
	switch level {
	case kinesis.LevelDebug:
		log.WithData(data).Debugf(format, args...)
	case kinesis.LevelInfo:
		log.WithData(data).Infof(format, args...)
	case kinesis.LevelError:
		log.WithData(data).Errorf(format, args...)
	}
}

// LogEvent logs events kinesis consumer.
func (l *Logger) LogEvent(event kinesis.EventLog) {
	log.WithData(logger.KV{"event": event.Event, "elapse": fmt.Sprintf("%v", event.Elapse)}).Debugf("event logger triggered")
}

func main() {
	log := &Logger{}

	config := kinesis.ConsumerConfig{
		Group:  "test-consumer",
		Stream: "stream-name-here",
		AWS: kinesis.AWSConfig{
			Endpoint: "http://localhost:4567",
			Region:   "eu-west-3",
		},
	}

	checkpoint := memory.NewCheckpoint()
	consumer, err := kinesis.NewConsumer(config, handler, checkpoint,
		kinesis.WithCheckpointStrategy(kinesis.AfterRecordBatch),
	)
	if err != nil {
		panic(err)
	}
	consumer.SetLogger(log)
	consumer.SetEventLogger(log)

	go func() {
		config := kinesis.ProducerConfig{
			Stream: "stream-name-here",
			AWS: kinesis.AWSConfig{
				Endpoint: "http://localhost:4567",
				Region:   "eu-west-3",
			},
		}

		messages := make([]kinesis.Message, 0, 10)
		producer, err := kinesis.NewProducer(config)
		if err != nil {
			panic(err)
		}

		for i := 0; i < 40; i++ {
			msg := fmt.Sprintf(`{"msg": "message received %d!!"}`, i)
			messages = append(messages, kinesis.Message{Partition: strconv.Itoa(i), Data: []byte(msg)})
		}

		if err := producer.PublishBatch(messages); err != nil {
			panic(err)
		}
	}()

	// listen for termination signals
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-termChan
		cancel()
	}()

	if err := consumer.Run(ctx); err != nil {
		panic(err)
	}
}
