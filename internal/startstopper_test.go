package internal

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	. "github.com/onsi/gomega"
)

var (
	nilHandler = func(stopChan chan bool) error {
		return nil
	}
)

func TestNewStartStopper(t *testing.T) {
	RegisterTestingT(t)

	startStopper := NewStartStopper(nilHandler)

	Expect(startStopper).ToNot(BeNil())
	Expect(startStopper.timeout).To(Equal(defaultTimeout))
	Expect(startStopper.handler).ToNot(BeNil())
	Expect(startStopper.stopChan).ToNot(BeNil())
	Expect(startStopper.childStopChan).ToNot(BeNil())
}

func TestNewStartStopper_WithHandler(t *testing.T) {
	RegisterTestingT(t)

	handler := func(stopChan chan bool) error {
		return nil
	}

	startStopper := NewStartStopper(handler)

	Expect(startStopper).ToNot(BeNil())
	Expect(startStopper.handler).ToNot(BeNil())
}

func TestNewStartStopper_WithTimeout(t *testing.T) {
	RegisterTestingT(t)

	startStopper := NewStartStopper(nilHandler, WithTimeout(time.Second))

	Expect(startStopper).ToNot(BeNil())
	Expect(startStopper.timeout).To(Equal(time.Second))
}

func TestStartStopper_Start_WithoutHandler(t *testing.T) {
	RegisterTestingT(t)

	startStopper := NewStartStopper(nilHandler)

	var g errgroup.Group
	g.Go(startStopper.Start)

	err := g.Wait()
	Expect(err).To(BeNil())
}

func TestStartStopper_Start_WithHandler(t *testing.T) {
	RegisterTestingT(t)

	handler := func(stopChan chan bool) error {
		<-stopChan

		return nil
	}

	startStopper := NewStartStopper(handler)

	var g errgroup.Group
	g.Go(startStopper.Start)

	time.Sleep(time.Second)
	startStopper.Stop()

	err := g.Wait()
	Expect(err).To(BeNil())
}

func TestStartStopper_Start_WithHandlerError(t *testing.T) {
	RegisterTestingT(t)

	handler := func(stopChan chan bool) error {
		return fmt.Errorf("look, an error")
	}

	startStopper := NewStartStopper(handler)

	var g errgroup.Group
	g.Go(startStopper.Start)

	time.Sleep(time.Second)
	startStopper.Stop()

	err := g.Wait()
	Expect(err).ToNot(BeNil())
}

func TestStartStopper_Stop_WithoutHandler(t *testing.T) {
	RegisterTestingT(t)

	startStopper := NewStartStopper(nilHandler)

	var g errgroup.Group
	g.Go(startStopper.Start)

	err := startStopper.Stop()
	Expect(err).To(BeNil())

	err = g.Wait()
	Expect(err).To(BeNil())
}

func TestStartStopper_Stop_WithHandler(t *testing.T) {
	RegisterTestingT(t)

	handler := func(stopChan chan bool) error {
		<-stopChan

		return nil
	}

	startStopper := NewStartStopper(handler)

	var g errgroup.Group
	g.Go(startStopper.Start)

	err := startStopper.Stop()
	Expect(err).To(BeNil())

	err = g.Wait()
	Expect(err).To(BeNil())
}

func TestStartStopper_Stop_WithHandlerAndTimeout(t *testing.T) {
	RegisterTestingT(t)

	handler := func(stopChan chan bool) error {
		time.Sleep(time.Second * 10)
		<-stopChan

		return nil
	}

	startStopper := NewStartStopper(handler, WithTimeout(time.Second))

	var g errgroup.Group
	g.Go(startStopper.Start)

	err := startStopper.Stop()
	Expect(err).To(BeNil())

	err = g.Wait()
	Expect(err).ToNot(BeNil())
}
