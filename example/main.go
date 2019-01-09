package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gitlab.com/vredens/go-logger"

	"gitlab.com/marcoxavier/go-kinesis/checkpoint/memory"

	"gitlab.com/marcoxavier/go-kinesis"
)

var log = logger.Spawn(logger.WithTags("kinesis-consumer"))

func handler(message kinesis.Message) error {
	fmt.Printf("partition: %s, data: %s\n", message.Partition, string(message.Data))

	return nil
}

// Log logs kinesis consumer.
func Log(level string, msg string, data map[string]interface{}) {
	switch level {
	case kinesis.Debug:
		log.WithData(data).Debug(msg)
	case kinesis.Info:
		log.WithData(data).Info(msg)
	case kinesis.Error:
		log.WithData(data).Error(msg)
	}
}

func main() {
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

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
		kinesis.WithLogger(Log),
	)
	if err != nil {
		panic(err)
	}

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

		time.Sleep(time.Second * 2)
		if err := producer.PublishBatch(messages); err != nil {
			panic(err)
		}
	}()

	errChan := make(chan error, 1)
	go func() {
		if err := consumer.Start(); err != nil {
			errChan <- err
		}
	}()

	select {
	case <-termChan:
		if err := consumer.Stop(); err != nil {
			panic(err)
		}
	case <-errChan:
		panic(err)
	}
}
