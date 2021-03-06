package kinesis

import (
	"time"

	validator "gopkg.in/validator.v2"
)

const (
	defaultStreamCheckTick      = time.Second * 30
	defaultRunnerFactoryTick    = time.Second * 10
	defaultRunnerTick           = time.Second * 5
	defaultRunnerGetRecordsRate = time.Millisecond * 250
)

// AWSConfig is a aws configuration.
type AWSConfig struct {
	Endpoint string `json:"endpoint" mapstructure:"endpoint"`
	Region   string `json:"region" mapstructure:"region"`
}

// ProducerConfig is a kinesis producer configuration.
type ProducerConfig struct {
	AWS    AWSConfig `json:"aws" mapstructure:"aws" validate:"nonzero"`
	Stream string    `json:"stream" mapstructure:"stream" validate:"nonzero"`
}

// validate validates kinesis configuration.
func (c *ProducerConfig) validate() error {
	return validator.Validate(c)
}

// ConsumerConfig is a kinesis consumer configuration.
type ConsumerConfig struct {
	AWS                  AWSConfig     `json:"aws" mapstructure:"aws"`
	Group                string        `json:"group" mapstructure:"group" validate:"nonzero"`
	Stream               string        `json:"stream" mapstructure:"stream" validate:"nonzero"`
	StreamCheckTick      time.Duration `json:"stream_tick" mapstructure:"stream_tick"`
	RunnerFactoryTick    time.Duration `json:"runner_factory_tick" mapstructure:"runner_factory_tick"`
	RunnerTick           time.Duration `json:"runner_tick" mapstructure:"runner_tick"`
	RunnerGetRecordsRate time.Duration `json:"runner_get_records_rate" mapstructure:"runner_get_records_rate"`
}

// validate validates kinesis configuration.
func (c *ConsumerConfig) validate() error {
	return validator.Validate(c)
}

// sanitize makes clean and hygienic.
func (c *ConsumerConfig) sanitize() {
	if c.StreamCheckTick <= 0 {
		c.StreamCheckTick = defaultStreamCheckTick
	}

	if c.RunnerFactoryTick <= 0 {
		c.RunnerFactoryTick = defaultRunnerFactoryTick
	}

	if c.RunnerTick <= 0 {
		c.RunnerTick = defaultRunnerTick
	}

	if c.RunnerGetRecordsRate <= 0 {
		c.RunnerGetRecordsRate = defaultRunnerGetRecordsRate
	}
}
