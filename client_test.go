package kinesis

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"

	. "github.com/onsi/gomega"
)

func TestClient_NewClient(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	config := AWSConfig{
		Endpoint: "some_endpoint",
		Region:   "some_region",
	}

	// Act
	client, err := NewClient(config)

	// Assert
	Expect(err).ToNot(HaveOccurred())
	Expect(client.Config.Endpoint).To(Equal(aws.String(config.Endpoint)))
	Expect(client.Config.Region).To(Equal(aws.String(config.Region)))
}
