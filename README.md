# Kinesis Client

[![pipeline status](https://gitlab.com/marcoxavier/go-kinesis/badges/master/pipeline.svg)](https://gitlab.com/marcoxavier/go-kinesis/commits/master) [![coverage report](https://gitlab.com/marcoxavier/go-kinesis/badges/master/coverage.svg)](https://gitlab.com/marcoxavier/go-kinesis/commits/master) [![Go Report Card](https://goreportcard.com/badge/gitlab.com/marcoxavier/go-kinesis)](https://goreportcard.com/report/gitlab.com/marcoxavier/go-kinesis) [![License MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://img.shields.io/badge/License-MIT-brightgreen.svg)

This library is wrapper around the Kinesis API to consume and produce records, saving the last sequence into a checkpoint abstraction.

## Features

* Iterator Strategy (SinceOldest and SinceLatest);
* Checkpoint Strategy (Saving each record or each batch);
* Handles stream deleting;
* Handles resharding in a pragmatic way;
* Handles throttling.
* Customizable timeouts/ticks;
* Override AWS Configuration;
* Some Checkpoint implementations out of the box;
* Logging;
* Fully tested;
* Being used in production environment.


## Installation

* go get
```bash
go get -u gitlab.com/marcoxavier/go-kinesis
```

* dep (github.com/golang/dep/cmd/dep)
```bash
dep ensure --add gitlab.com/marcoxavier/go-kinesis
```

## Usage

### Consumer

All you need is to pass a consumer configuration, inject the checkpoint implementation and send you message handler.

```go
handler := func handler(message kinesis.Message) error {
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

> __Warning__: It's not recoment to publish in batch, some records can fail to be published.
> At the moment it's not being handler correctly.

### Tools

__Tail__:  Consumes the specified stream and writes to stdout all records after the most recent record in the shard.

__Head__: Consumes the specified stream and writes to stdout all untrimmed records in the shard.

In order to install `kinesis tools` you need some requirements:
* go version 1.11

Kinesis tool uses your specified AWS profile (read [THIS](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) for more information)

For more information, nothing better than using the help parameter.
```bash
kinesis --help
```

Example
```bash
me@ubuntu:~$ go install gitlab.com/marcoxavier/go-kinesis/tools/kinesis
me@ubuntu:~$ export AWS_PROFILE=some_aws_profile_here
me@ubuntu:~$ kinesis head -s some_stream -n 20
```


## Future Improvements

- [ ] Handle correctly will publishing a batch
- [ ] Split or merge runners while resharding
- [x] Decouple logger
- [ ] Add metrics