package main

import (
	"os"

	"gitlab.com/vredens/go-logger"

	"gitlab.com/marcoxavier/go-kinesis/tools/kinesis/head"
	"gitlab.com/marcoxavier/go-kinesis/tools/kinesis/tail"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{Use: "kinesis COMMAND"}
	rootCmd.AddCommand(tail.Command())
	rootCmd.AddCommand(head.Command())

	if err := rootCmd.Execute(); err != nil {
		logger.Errorf("error during initialization")
		os.Exit(1)
	}
}
