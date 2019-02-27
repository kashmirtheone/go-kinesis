package main

import (
	"context"
	"fmt"
	"strconv"

	logger "gitlab.com/vredens/go-logger"

	"gitlab.com/marcoxavier/go-kinesis/checkpoint/memory"
	"gitlab.com/marcoxavier/supervisor"

	kinesis "gitlab.com/marcoxavier/go-kinesis"
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
	s := supervisor.NewSupervisor()
	s.SetLogger(log.Log)

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

	s.AddRunner("kinesis-consumer", consumer.Run)

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

	s.Start()
}
