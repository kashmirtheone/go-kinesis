package worker

import (
	"fmt"

	"gitlab.com/marcoxavier/go-kinesis/internal"

	"sync/atomic"

	"github.com/pkg/errors"
)

const (
	// StateReady is the state a worker is in after being initialized.
	StateReady = iota
	// StateRunning is when the worker has started successfully.
	StateRunning
	// StateProcessing is when the worker has processing tasks.
	StateProcessing
	// StateStopping is when a Stop command has been requested but the worker hasn't stopped yet. A worker in this state refuses to add more tasks to the queue.
	StateStopping
	// StateDying is set when a Kill command is issued. This prevents further tasks from being added to the queue while the fatal process is under way.
	StateDying
	// StateDead happens when a worker has been killed.
	StateDead
)

// Option is the abstract functional-parameter type used for worker configuration.
type Option func(*Worker)

// Handler process to run each tick.
// If Handler return an error, worker will stop.
type Handler func() error

// Notifier defines when and how frequent worker will order something to process.
type Notifier interface {
	Notify() <-chan bool
	Start() error
	Stop() error
}

// Worker is responsible to order to processes something depending on notifier strategy.
type Worker struct {
	internal.StartStopper

	notifier    Notifier
	handler     Handler
	state       int32
	logger      func(level string, msg string, data map[string]interface{})
	stopCommand chan bool
}

// WithNotifier allows you to configure the notifier.
func WithNotifier(notifier Notifier) Option {
	return func(r *Worker) {
		if notifier != nil {
			r.notifier = notifier
		}
	}
}

// WithLogger allows you to configure the logger.
func WithLogger(logger func(level string, msg string, data map[string]interface{})) Option {
	return func(r *Worker) {
		if logger != nil {
			r.logger = logger
		}
	}
}

// NewWorker create a new worker
func NewWorker(handler Handler, opts ...Option) Worker {
	w := Worker{
		handler:     handler,
		stopCommand: make(chan bool),
		logger:      func(level string, msg string, data map[string]interface{}) {},
		state:       StateReady,
	}
	w.StartStopper = internal.NewStartStopper(w.process)

	for _, opt := range opts {
		opt(&w)
	}

	return w
}

// Start starts the process.
func (w *Worker) Start() error {
	if err := w.notifier.Start(); err != nil {
		return errors.Wrap(err, "failed to start worker notifier")
	}

	atomic.SwapInt32(&w.state, StateRunning)

	return w.StartStopper.Start()
}

// Stop stops the process
func (w *Worker) Stop() error {
	if atomic.LoadInt32(&w.state) == StateReady {
		return errors.New("service is not running")
	}

	atomic.SwapInt32(&w.state, StateStopping)

	if err := w.notifier.Stop(); err != nil {
		w.logger("info", "failed to stop worker notifier", map[string]interface{}{"cause": fmt.Sprintf("%v", err)})
	}

	if err := w.StartStopper.Stop(); err != nil {
		return err
	}

	atomic.SwapInt32(&w.state, StateReady)

	return nil
}

// State returns current state.
func (w *Worker) State() int32 {
	return atomic.LoadInt32(&w.state)
}

func (w *Worker) process(stopChan chan bool) error {
	for {
		select {
		case <-w.notifier.Notify():
			atomic.SwapInt32(&w.state, StateProcessing)

			if err := w.handler(); err != nil {
				return err
			}

			atomic.SwapInt32(&w.state, StateRunning)
		case <-stopChan:
			return nil
		}
	}
}
