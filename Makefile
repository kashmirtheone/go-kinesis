.PHONY: mocks test coverage clean
.SILENT:

install:
	@echo "=== Installing dependencies ==="
	@dep ensure -v

mocks: clean
	@echo "=== Creating mocks ==="
	CGO_ENABLED=0 mockery -name . -inpkg
	CGO_ENABLED=0 mockery -name KinesisAPI -dir vendor/github.com/aws/aws-sdk-go/service/kinesis/kinesisiface -output . -outpkg kinesis
	CGO_ENABLED=0 mockery -name Error -dir vendor/github.com/aws/aws-sdk-go/aws/awserr -output . -outpkg kinesis

test: mocks
	@echo "=== Running Unit Tests ==="
	go test -race -coverprofile=.coverage.out.tmp  ./...
	cat .coverage.out.tmp | grep -v 'mock_\|KinesisAPI.go\|Error.go' > .coverage.out
	go tool cover -func=.coverage.out

clean:
	@rm -rf mock_*.go KinesisAPI.go Error.go