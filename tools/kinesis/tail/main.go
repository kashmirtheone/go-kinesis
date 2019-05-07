package tail

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/pkg/errors"

	logger "gitlab.com/vredens/go-logger"

	"github.com/kashmirtheone/go-kinesis/checkpoint/memory"

	kinesis "github.com/kashmirtheone/go-kinesis"

	"github.com/spf13/cobra"
)

var (
	termChan  = make(chan os.Signal, 1)
	iteration int32
	log       = logger.Spawn(logger.ConfigTags("consumer"))

	stream             string
	endpoint           string
	region             string
	number             int
	logging            bool
	gzipDecode         bool
	skiReshardingOrder bool
	sequence           string
	shard              string
)

// Command creates a new command.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "The tail utility displays the contents of kinesis stream to the standard output, starting in the latest record.",
		RunE:  Run,
	}
	cmd.Flags().StringVarP(&stream, "stream", "s", "", "stream name")
	cmd.Flags().StringVarP(&endpoint, "endpoint", "e", "", "kinesis endpoint")
	cmd.Flags().StringVarP(&region, "region", "r", "", "aws region, by default it will use AWS_REGION from aws config")
	cmd.Flags().IntVarP(&number, "number", "n", 0, "number of messages to show")
	cmd.Flags().BoolVar(&logging, "logging", false, "enables logging, mute by default")
	cmd.Flags().BoolVar(&gzipDecode, "gzip", false, "enables gzip decoder")
	cmd.Flags().BoolVar(&skiReshardingOrder, "skip-resharding-order", false, "if enabled, consumer will skip ordering when resharding")
	cmd.Flags().StringVarP(&sequence, "sequence", "", "", "specific sequence number, you also need to define a shard id ")
	cmd.Flags().StringVarP(&shard, "shard", "", "", "shard id ")

	return cmd
}

// Run runs kinesis tail
func Run(cmd *cobra.Command, args []string) error {
	if err := os.Setenv("AWS_SDK_LOAD_CONFIG", "1"); err != nil {
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

	var skiReshardingOrderOption = dumbConsumerOption()
	if skiReshardingOrder {
		skiReshardingOrderOption = kinesis.SkipReshardingOrder()
	}

	var sequenceOption = dumbConsumerOption()
	if sequence != "" {
		if shard == "" {
			return fmt.Errorf("you need to specify a shard id")
		}

		sequenceOption = kinesis.SinceSequence(shard, sequence)
	}

	checkpoint := memory.NewCheckpoint()
	consumer, err := kinesis.NewConsumer(config, handler(), checkpoint,
		kinesis.WithCheckpointStrategy(kinesis.AfterRecordBatch),
		kinesis.SinceLatest(),
		skiReshardingOrderOption,
		sequenceOption,
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
	case kinesis.LevelDebug:
		log.WithData(data).Debugf(format, args...)
	case kinesis.LevelInfo:
		log.WithData(data).Infof(format, args...)
	case kinesis.LevelError:
		log.WithData(data).Errorf(format, args...)
	}
}

func handler() kinesis.MessageHandler {
	var f = bufio.NewWriter(os.Stdout)

	return func(_ context.Context, message kinesis.Message) error {
		if !(number > 0 && atomic.LoadInt32(&iteration) >= int32(number)) {

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
