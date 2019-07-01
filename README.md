# Kinesis Client

[![Build Status](https://travis-ci.com/kashmirtheone/go-kinesis.svg?branch=master)](https://travis-ci.com/kashmirtheone/go-kinesis)
[![GoDoc](https://godoc.org/github.com/kashmirtheone/go-kinesis?status.svg)](https://godoc.org/github.com/kashmirtheone/go-kinesis)
[![Go Report Card](https://goreportcard.com/badge/gitlab.com/marcoxavier/go-kinesis)](https://goreportcard.com/report/gitlab.com/marcoxavier/go-kinesis)
[![License MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://img.shields.io/badge/License-MIT-brightgreen.svg)

This library is wrapper around the Kinesis API to consume and produce records, saving the last sequence into a checkpoint abstraction.

## Features

* Iterator Strategy (SinceOldest and SinceLatest);
* Checkpoint Strategy (Saving each record or each batch);
* Handles stream deleting;
* Handles stream resharding, optionally you can set if you want to keep shard ordering or not;
* Handles throttling.
* Customizable timeouts/ticks;
* Override AWS Configuration;
* Some Checkpoint implementations out of the box;
* Logging;
* Event Logger
* Metrics
* Fully tested;
* Being used in production environment.
* Etc...


## Installation

* go get
```bash
go get -u github.com/kashmirtheone/go-kinesis
```

* dep (github.com/golang/dep/cmd/dep)
```bash
dep ensure --add github.com/kashmirtheone/go-kinesis
```

## Usage

### Consumer

All you need is to pass a consumer configuration, inject the checkpoint implementation and send you message handler.

```go
handler := func handler(_ context.Context, message kinesis.Message) error {
	fmt.Println(string(message.Data))
	return nil
}

config := kinesis.ConsumerConfig{
    Group:  "my-test-consumer",
    Stream: "stream-name-here"
}

checkpoint := memory.NewCheckpoint()
consumer, err := kinesis.NewConsumer(config, handler, checkpoint)
if err != nil {
	return errors.Wrap(err, "failed to create kinesis consumer")
}

if err := consumer.Start(); err != nil {
    return errors.Wrap(err, "failed to start kinesis consumer")
}
```

Shard iteration is handled in handler error.
If error is nil, it will continue to iterate through shard.

### Producer

```go
config := kinesis.ProducerConfig{
    Stream: "stream-name-here",
}

producer, err := kinesis.NewProducer(config)
if err != nil {
    return errors.Wrap(err, "failed to create kinesis consumer")
}

message := kinesis.Message{
    Data: []byte("some-data"), 
    Partition: "some-partition",
}

if err := producer.Publish(message); err != nil {
    return errors.Wrap(err, "failed to publish message")
}
```

> __Warning__: It's not recommended to publish in batch, some records can fail to be published.
> At the moment it's not being handler correctly.

### Tools

__Tail__:  Consumes the specified stream and writes to stdout all records after the most recent record in the shard.

__Head__: Consumes the specified stream and writes to stdout all untrimmed records in the shard.

Kinesis tool uses your specified AWS profile (read [THIS](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) for more information)

For more information, nothing better than using the help parameter.
```bash
me@ubuntu:~$ go-kinesis --help
Usage:
  go-kinesis [command]

Available Commands:
  head        The head utility displays the contents of kinesis stream to the standard output, starting in the oldest record.
  help        Help about any command
  tail        The tail utility displays the contents of kinesis stream to the standard output, starting in the latest record.

Flags:
  -h, --help   help for kinesis

Use "kinesis [command] --help" for more information about a command.
```

```bash
me@ubuntu:~$ go-kinesis tail --help
The tail utility displays the contents of kinesis stream to the standard output, starting in the latest record.

Usage:
  go-kinesis tail [flags]

Flags:
  -e, --endpoint string         kinesis endpoint
      --gzip                    enables gzip decoder
  -h, --help                    help for tail
      --logging                 enables logging, mute by default
  -n, --number int              number of messages to show
  -r, --region string           aws region, by default it will use AWS_REGION from aws config
      --skip-resharding-order   if enabled, consumer will skip ordering when resharding
  -s, --stream string           stream name
```

Example
```bash
me@ubuntu:~$ go get -u github.com/kashmirtheone/go-kinesis
me@ubuntu:~$ export AWS_PROFILE=some_aws_profile_here
me@ubuntu:~$ go-kinesis head -s some_stream -n 20
```


## Future Improvements

- [x] Split or merge runners while resharding
- [x] Decouple logger
- [x] Add metrics
- [ ] Handle correctly while publishing a batch
