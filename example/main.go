package main

import (
	"fmt"
	"strconv"

	logger "gitlab.com/vredens/go-logger"

	"gitlab.com/marcoxavier/go-kinesis/checkpoint/memory"
	"gitlab.com/marcoxavier/supervisor"

	kinesis "gitlab.com/marcoxavier/go-kinesis"
)

var log = logger.Spawn(logger.WithTags("kinesis-consumer"))

func handler(message kinesis.Message) error {
	fmt.Printf("partition: %s, data: %s\n", message.Partition, string(message.Data))

	return nil
}

// Log logs kinesis consumer.
func Log(level string, data map[string]interface{}, format string, args ...interface{}) {
	switch level {
	case kinesis.Debug:
		log.WithData(data).Debugf(format, args...)
	case kinesis.Info:
		log.WithData(data).Infof(format, args...)
	case kinesis.Error:
		log.WithData(data).Errorf(format, args...)
	}
}

func main() {
	s := supervisor.NewSupervisor()
	s.SetLogger(Log)

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
		kinesis.WithCheckpointStrategy(kinesis.AfterRecord),
	)
	if err != nil {
		panic(err)
	}
	consumer.SetLogger(Log)

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
