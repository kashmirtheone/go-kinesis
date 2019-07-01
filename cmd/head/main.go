package head

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/kashmirtheone/go-kinesis/checkpoint/memory"
	"github.com/pkg/errors"

	kinesis "github.com/kashmirtheone/go-kinesis/kinesis"

	logger "gitlab.com/vredens/go-logger/v2"

	"github.com/spf13/cobra"
)

var (
	termChan  = make(chan os.Signal, 1)
	iteration int32
	log       = logger.Spawn().WithTags("tail")

	stream             string
	endpoint           string
	region             string
	number             int
	logging            bool
	gzipDecode         bool
	skiReshardingOrder bool
)

// Command creates a new command.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "head",
		Short:   "The head utility displays the contents of kinesis stream to the standard output, starting in the oldest record.",
		RunE:    Run,
		PreRunE: PreRun,
	}

	return cmd
}

// PreRun pre runs command.
func PreRun(cmd *cobra.Command, args []string) (err error) {
	stream, err = cmd.Flags().GetString("stream")
	if err != nil {
		return err
	}

	endpoint, err = cmd.Flags().GetString("endpoint")
	if err != nil {
		return err
	}

	region, err = cmd.Flags().GetString("region")
	if err != nil {
		return err
	}

	number, err = cmd.Flags().GetInt("number")
	if err != nil {
		return err
	}

	logging, err = cmd.Flags().GetBool("logging")
	if err != nil {
		return err
	}

	gzipDecode, err = cmd.Flags().GetBool("gzip")
	if err != nil {
		return err
	}

	skiReshardingOrder, err = cmd.Flags().GetBool("skip-resharding-order")
	if err != nil {
		return err
	}

	return nil
}

// Run runs kinesis head
func Run(_ *cobra.Command, _ []string) error {
	if err := os.Setenv("AWS_SDK_LOAD_CONFIG", "1"); err != nil {
		return err
	}

	config := kinesis.ConsumerConfig{
		Group:  "head",
		Stream: stream,
		AWS: kinesis.AWSConfig{
			Endpoint: endpoint,
			Region:   region,
		},
	}

	var skiReshardingOrderOption = dumbConsumerOption
	if skiReshardingOrder {
		skiReshardingOrderOption = kinesis.SkipReshardingOrder
	}

	checkpoint := memory.NewCheckpoint()
	consumer, err := kinesis.NewConsumer(config, handler(), checkpoint,
		kinesis.WithCheckpointStrategy(kinesis.AfterRecordBatch),
		kinesis.SinceOldest(),
		skiReshardingOrderOption(),
	)
	if err != nil {
		return err
	}

	if logging {
		l := &Logger{}
		consumer.SetLogger(l)
	}

	// listen for termination signals
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-termChan
		cancel()
	}()

	if err := consumer.Run(ctx); err != nil {
		return err
	}

	return nil
}

// Logger holds logger
type Logger struct {
}

// Log logs kinesis consumer.
func (l *Logger) Log(level string, data map[string]interface{}, format string, args ...interface{}) {
	switch level {
	case kinesis.LevelDebug, kinesis.LevelInfo:
		log.WithData(data).Debug().Write(format, args...)
	case kinesis.LevelError:
		log.WithData(data).Write(format, args...)
	}
}

func handler() kinesis.MessageHandler {
	var f = bufio.NewWriter(os.Stdout)

	return func(_ context.Context, message kinesis.Message) error {
		if number > 0 && atomic.LoadInt32(&iteration) >= int32(number) {
			return nil
		}

		msg := message.Data
		if gzipDecode {
			reader, err := gzip.NewReader(bytes.NewBuffer(message.Data))
			if err != nil {
				return errors.Wrap(err, "failed to decode message")
			}

			msg, err = ioutil.ReadAll(reader)
			if err != nil {
				return errors.Wrap(err, "failed to read decoded message")
			}
		}

		f.WriteString(string(msg) + "\n")
		f.Flush()

		atomic.AddInt32(&iteration, 1)
		if number > 0 && atomic.LoadInt32(&iteration) >= int32(number) {
			select {
			case termChan <- os.Interrupt:
				return nil
			default:
				return nil
			}
		}

		return nil
	}
}

func dumbConsumerOption() kinesis.ConsumerOption {
	return func(c *kinesis.ConsumerOptions) {}
}
