package pipeline

import (
	"time"
)

type Step interface {
	Exec(interface{}) interface{}
}

type DelayStep struct {
	delay time.Duration
}

func NewDelayStep(delay time.Duration) *DelayStep {
	return &DelayStep{
		delay: delay,
	}
}

func (s *DelayStep) Exec(x interface{}) interface{} {
	time.Sleep(s.delay)
	return x
}

type SourceDelayStep struct {
	// This leaks because we never call Stop() on it...
	ticker    *time.Ticker
	counter   int
	increment int
}

func NewSourceDelayStep(delay time.Duration) *SourceDelayStep {
	return &SourceDelayStep{
		ticker:    time.NewTicker(delay),
		increment: 1,
	}
}

func (s *SourceDelayStep) SetInitial(x int) {
	s.counter = x
}

func (s *SourceDelayStep) SetIncrement(x int) {
	s.increment = x
}

func (s *SourceDelayStep) Exec(_ interface{}) interface{} {
	<-s.ticker.C
	x := s.counter
	s.counter += s.increment
	return Item{Id: x, Start: time.Now()}
}
