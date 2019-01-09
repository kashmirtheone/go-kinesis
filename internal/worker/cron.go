package worker

import "time"

// CronNotifier is a cron notifier that ticks every second configured
type CronNotifier struct {
	stopChan chan bool
	tick     chan bool
	interval time.Duration
}

// NewCronNotifier creates a new cron notifier
func NewCronNotifier(interval time.Duration) *CronNotifier {
	return &CronNotifier{
		stopChan: make(chan bool),
		tick:     make(chan bool),
		interval: interval,
	}
}

// Notify return an channel to tick every 'interval' duration
func (c *CronNotifier) Notify() <-chan bool {
	return c.tick
}

// Start starts the notifier
func (c *CronNotifier) Start() error {
	go func() {
		c.tick <- true
		for {
			select {
			case <-time.After(c.interval):
				c.tick <- true
			case <-c.stopChan:
				return
			}
		}
	}()

	return nil
}

// Stop stops the notifier
func (c *CronNotifier) Stop() error {
	c.stopChan <- true

	return nil
}
