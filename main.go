package main

import (
	"os"

	"github.com/kashmirtheone/go-kinesis/cmd/head"
	"github.com/kashmirtheone/go-kinesis/cmd/tail"
	"github.com/spf13/cobra"
	logger "gitlab.com/vredens/go-logger/v2"
)

var (
	stream             string
	endpoint           string
	region             string
	number             int
	logging            bool
	gzipDecode         bool
	skiReshardingOrder bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:           "go-kinesis",
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       PreRun,
	}

	rootCmd.AddCommand(tail.Command())
	rootCmd.AddCommand(head.Command())

	rootCmd.PersistentFlags().StringVarP(&stream, "stream", "s", "", "stream name")
	rootCmd.PersistentFlags().StringVarP(&endpoint, "endpoint", "e", "", "kinesis endpoint")
	rootCmd.PersistentFlags().StringVarP(&region, "region", "r", "", "aws region, by default it will use AWS_REGION from aws config")
	rootCmd.PersistentFlags().IntVarP(&number, "number", "n", 0, "number of messages to show")
	rootCmd.PersistentFlags().BoolVar(&logging, "logging", false, "enables logging, mute by default")
	rootCmd.PersistentFlags().BoolVar(&gzipDecode, "gzip", false, "enables gzip decoder")
	rootCmd.PersistentFlags().BoolVar(&skiReshardingOrder, "skip-resharding-order", false, "if enabled, consumer will skip ordering when resharding")

	_ = rootCmd.MarkPersistentFlagRequired("stream")

	if err := rootCmd.Execute(); err != nil {
		rootCmd.Printf("ERR: %v", err)
	}
}

// PreRun pre runs command.
func PreRun(_ *cobra.Command, _ []string) error {
	logger.DebugMode(logging)
	if err := os.Setenv("AWS_SDK_LOAD_CONFIG", "1"); err != nil {
		return err
	}

	return nil
}
