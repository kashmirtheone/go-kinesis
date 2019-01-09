package internal

import (
	"time"

	"github.com/pkg/errors"
)

const (
	defaultTimeout = 30 * time.Second
)

// StartStopperOption is the abstract functional-parameter type used for StartStopper configuration.
type StartStopperOption func(*StartStopper)

// StartStopperHandler is the process handler that receives a stop channel that will be notified when StarStopper
// stops.
type StartStopperHandler func(stop chan bool) error

// StartStopper is a helper that abstracts Start and Stop logic.
type StartStopper struct {
	timeout       time.Duration
	handler       StartStopperHandler
	childStopChan chan bool
	stopChan      chan bool
}

// NewStartStopper creates a new StartStopper.
func NewStartStopper(handler StartStopperHandler, options ...StartStopperOption) StartStopper {
	p := StartStopper{
		handler:       handler,
		timeout:       defaultTimeout,
		childStopChan: make(chan bool),
		stopChan:      make(chan bool),
	}

	for _, opt := range options {
		opt(&p)
	}

	return p
}

// WithTimeout allows you to configure the timeout.
func WithTimeout(timeout time.Duration) StartStopperOption {
	return func(p *StartStopper) {
		if timeout > 0 {
			p.timeout = timeout
		}
	}
}

// Start starts the process and is blocking.
func (p *StartStopper) Start() error {
	if p.handler == nil {
		return errors.New("no handler associated")
	}

	var errChan = make(chan error)

	go func() {
		errChan <- p.handler(p.childStopChan)
	}()

	select {
	case <-p.stopChan:
		go p.stopChild()
		select {
		case err := <-errChan:
			if err != nil {
				return errors.Wrap(err, "process terminated in error")
			}
			return nil
		case <-time.After(p.timeout):
			return errors.New("process timed out to stop, killing process")
		}
	case err := <-errChan:
		if err != nil {
			return errors.Wrap(err, "failed to start process")
		}
	}

	return nil
}

// Stop stops the process.
func (p *StartStopper) Stop() error {
	close(p.stopChan)

	return nil
}

func (p *StartStopper) stopChild() {
	p.childStopChan <- true
}
