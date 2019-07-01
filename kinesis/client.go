package kinesis

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
)

// NewClient creates a new kinesis client
func NewClient(config AWSConfig) (*kinesis.Kinesis, error) {
	c := aws.NewConfig()

	if config.Endpoint != "" {
		c.WithEndpoint(config.Endpoint)
	}
	if config.Region != "" {
		c.WithRegion(config.Region)
	}

	awsSession, err := session.NewSession(c)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a new aws session")
	}

	return kinesis.New(awsSession), nil
}
