package kinesis

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestConfig_ProducerConfig_Validate_MissingStream(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	config := ProducerConfig{
		Stream: "",
		AWS: AWSConfig{
			Endpoint: "some_endpoint",
			Region:   "some_region",
		},
	}

	// Act
	err := config.validate()

	// Assert
	Expect(err).To(HaveOccurred())
}

func TestConfig_ProducerConfig_Validate_Valid(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	config := ProducerConfig{
		Stream: "some_stream",
		AWS: AWSConfig{
			Endpoint: "some_endpoint",
			Region:   "some_region",
		},
	}

	// Act
	err := config.validate()

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestConfig_ConsumerConfig_Validate_MissingGroup(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	config := ConsumerConfig{
		Stream: "",
		Group:  "some_group",
		AWS: AWSConfig{
			Endpoint: "some_endpoint",
			Region:   "some_region",
		},
	}

	// Act
	err := config.validate()

	// Assert
	Expect(err).To(HaveOccurred())
}

func TestConfig_ConsumerConfig_Validate_MissingStream(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	config := ConsumerConfig{
		Stream: "",
		Group:  "some_group",
		AWS: AWSConfig{
			Endpoint: "some_endpoint",
			Region:   "some_region",
		},
	}

	// Act
	err := config.validate()

	// Assert
	Expect(err).To(HaveOccurred())
}

func TestConfig_ConsumerConfig_Validate_Valid(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	config := ConsumerConfig{
		Stream: "some_stream",
		Group:  "some_group",
		AWS: AWSConfig{
			Endpoint: "some_endpoint",
			Region:   "some_region",
		},
	}

	// Act
	err := config.validate()

	// Assert
	Expect(err).ToNot(HaveOccurred())
}

func TestConfig_ConsumerConfig_Sanitize_All(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	config := ConsumerConfig{
		Stream: "some_stream",
		Group:  "some_group",
		AWS: AWSConfig{
			Endpoint: "some_endpoint",
			Region:   "some_region",
		},
	}

	// Act
	config.sanitize()

	// Assert
	Expect(config.RunnerFactoryTick).To(Equal(defaultRunnerFactoryTick))
	Expect(config.StreamCheckTick).To(Equal(defaultStreamCheckTick))
	Expect(config.RunnerTick).To(Equal(defaultRunnerTick))
}

func TestConfig_ConsumerConfig_Sanitize_None(t *testing.T) {
	RegisterTestingT(t)

	// Assign
	config := ConsumerConfig{
		Stream:            "some_stream",
		Group:             "some_group",
		RunnerFactoryTick: time.Minute,
		StreamCheckTick:   time.Minute,
		RunnerTick:        time.Minute,
		AWS: AWSConfig{
			Endpoint: "some_endpoint",
			Region:   "some_region",
		},
	}

	// Act
	config.sanitize()

	// Assert
	Expect(config.RunnerFactoryTick).To(Equal(config.RunnerFactoryTick))
	Expect(config.StreamCheckTick).To(Equal(config.StreamCheckTick))
	Expect(config.RunnerTick).To(Equal(config.RunnerTick))
}
