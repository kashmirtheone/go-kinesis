.PHONY: mocks test coverage
.SILENT:

install:
	@echo "=== Installing dependencies ==="
	@dep ensure -v

mocks:
	@echo "=== Creating mocks ==="
	rm -fr mocks
	CGO_ENABLED=0 mockery -name . -outpkg mocks -output mocks
	CGO_ENABLED=0 mockery -name KinesisAPI -dir vendor/github.com/aws/aws-sdk-go/service/kinesis/kinesisiface -outpkg mocks -output mocks

test: mocks
	@echo "=== Running Unit Tests ==="
	go test -race -coverprofile=.coverage.out  ./...
	go tool cover -func=.coverage.out