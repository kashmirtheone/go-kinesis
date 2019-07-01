.PHONY: mocks test coverage clean
.SILENT:

install:
	@echo "=== Installing dependencies ==="
	@go mod tidy

mocks: clean
	@echo "=== Creating mocks ==="
	mockgen -self_package github.com/kashmirtheone/go-kinesis/kinesis -package kinesis -destination kinesis/mocks_consumer.go github.com/kashmirtheone/go-kinesis/kinesis StreamChecker,RunnerFactory,Checkpoint,Logger,EventLogger,Runner
	mockgen -package kinesis -destination kinesis/mocks_kinesisiface.go github.com/aws/aws-sdk-go/service/kinesis/kinesisiface KinesisAPI
	mockgen -package kinesis -destination kinesis/mocks_awserr.go github.com/aws/aws-sdk-go/aws/awserr Error

test: mocks
	@echo "=== Running Unit Tests ==="
	go test -race -coverprofile=.coverage.out.tmp  ./kinesis/...
	cat .coverage.out.tmp | grep -v 'mocks_' > .coverage.out
	go tool cover -func=.coverage.out

clean:
	@rm -rf kinesis/mocks_*.go