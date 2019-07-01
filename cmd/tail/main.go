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

	logger "gitlab.com/vredens/go-logger/v2"

	"github.com/kashmirtheone/go-kinesis/checkpoint/memory"

	kinesis "github.com/kashmirtheone/go-kinesis/kinesis"

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
	sequence           string
	shard              string
)

// Command creates a new command.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "tail",
		Short:   "The tail utility displays the contents of kinesis stream to the standard output, starting in the latest record.",
		RunE:    Run,
		PreRunE: PreRun,
	}

	cmd.Flags().StringVar(&sequence, "sequence", "", "specific sequence number, you also need to define a shard id")
	cmd.Flags().StringVar(&shard, "shard", "", "shard id")

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

	sequence, err = cmd.Flags().GetString("sequence")
	if err != nil {
		return err
	}

	shard, err = cmd.Flags().GetString("shard")
	if err != nil {
		return err
	}

	return nil
}

// Run runs kinesis tail
func Run(_ *cobra.Command, _ []string) error {
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
