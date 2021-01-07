package pipeline

import (
	"fmt"
	"os"
)

type Source struct {
	id        string
	stepFn    func() Step
	out       chan interface{}
	cancelled bool
	limit     int
}

func NewSource(id string, stepFn func() Step) *Source {
	return &Source{
		id:     id,
		stepFn: stepFn,
		out:    make(chan interface{}),
	}
}

func (s *Source) SetLimit(limit int) {
	s.limit = limit
}

func (s *Source) Out() <-chan interface{} {
	return s.out
}

func (s *Source) Cancel() {
	s.cancelled = true
}

func (s *Source) Run(done <-chan struct{}) {
	defer close(s.out)
	step := s.stepFn()
	for {
		if s.cancelled {
			break
		}
		x := step.Exec(nil).(Item)
		if s.limit > 0 && x.Id >= s.limit {
			break
		}
		select {
		case s.out <- x:
		case <-done:
			fmt.Fprintf(os.Stderr, "%s: Received done signal, aborting write\n", s.id)
			return
		}
	}
}
