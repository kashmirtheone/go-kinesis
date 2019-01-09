package tail

import (
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"gitlab.com/vredens/go-logger"

	"gitlab.com/marcoxavier/go-kinesis/checkpoint/memory"

	"gitlab.com/marcoxavier/go-kinesis"

	"github.com/spf13/cobra"
)

var (
	termChan  = make(chan os.Signal, 1)
	iteration int32

	log = logger.Spawn(logger.WithTags("consumer"))
)

// Command creates a new command.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "The tail utility displays the contents of kinesis stream to the standard output, starting in the latest record.",
		RunE:  Run,
	}
	cmd.Flags().StringP("stream", "s", "", "stream name")
	cmd.Flags().StringP("endpoint", "e", "", "kinesis endpoint")
	cmd.Flags().StringP("region", "r", "", "aws region, by default it will use AWS_REGION from aws config")
	cmd.Flags().IntP("number", "n", 0, "number of messages to show")
	cmd.Flags().Bool("logging", false, "enables logging, mute by default")

	return cmd
}

// Run runs kinesis tail
func Run(cmd *cobra.Command, args []string) error {
	if err := os.Setenv("AWS_SDK_LOAD_CONFIG", "1"); err != nil {
		return err
	}

	// listen for termination signals
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	stream, err := cmd.Flags().GetString("stream")
	if err != nil {
		return err
	}
	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		return err
	}
	region, err := cmd.Flags().GetString("region")
	if err != nil {
		return err
	}
	number, err := cmd.Flags().GetInt("number")
	if err != nil {
		return err
	}
	logging, err := cmd.Flags().GetBool("logging")
	if err != nil {
		return err
	}

	config := kinesis.ConsumerConfig{
		Group:  "tail",
		Stream: stream,
		AWS: kinesis.AWSConfig{
			Endpoint: endpoint,
			Region:   region,
		},
	}

	log := kinesis.DumbLogger
	if logging {
		log = Log
	}

	checkpoint := memory.NewCheckpoint()
	consumer, err := kinesis.NewConsumer(config, handler(number), checkpoint,
		kinesis.WithCheckpointStrategy(kinesis.AfterRecord),
		kinesis.SinceLatest(),
		kinesis.WithLogger(log),
	)
	if err != nil {
		return err
	}

	errChan := make(chan error, 1)
	go func() {
		if err := consumer.Start(); err != nil {
			errChan <- err
		}
	}()

	select {
	case <-termChan:
		if err := consumer.Stop(); err != nil {
			return err
		}
	case <-errChan:
		return err
	}

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

func handler(number int) kinesis.MessageHandler {
	return func(message kinesis.Message) error {
		if number != 0 && atomic.LoadInt32(&iteration) >= int32(number) {
			select {
			case termChan <- os.Interrupt:
				return nil
			default:
				return nil
			}
		}

		fmt.Println(string(message.Data))
		atomic.AddInt32(&iteration, 1)

		return nil
	}
}
