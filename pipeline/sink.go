package pipeline

import (
	"errors"
	"fmt"
	"time"
)

type Sink struct {
	id         string
	pool       chan Step
	numWorkers int
	maxWorkers int
	stepFn     func() Step
	done       chan struct{}
	cancelled  bool
}

func NewSink(id string, stepFn func() Step) *Sink {
	maxWorkers := 20
	sink := &Sink{
		id:         id,
		pool:       make(chan Step, maxWorkers),
		maxWorkers: maxWorkers,
		stepFn:     stepFn,
		done:       make(chan struct{}),
	}
	sink.ScaleUp()
	return sink
}

func (s *Sink) Done() <-chan struct{} {
	return s.done
}

// Cancel will close the Done channel. It should only be called by the
// goroutine that created the Stage.
func (s *Sink) Cancel() {
	if !s.cancelled {
		close(s.done)
		s.cancelled = true
	}
}

func (s *Sink) ScaleUp() error {
	if s.numWorkers == s.maxWorkers {
		return errors.New("scale-up limit reached")
	}
	s.pool <- s.stepFn()
	s.numWorkers += 1
	return nil
}

func (s *Sink) ScaleDown() error {
	if s.numWorkers == 0 {
		return errors.New("scale-down limit reached")
	}
	<-s.pool
	s.numWorkers -= 1
	return nil
}

func (s *Sink) Drain() {
	for s.ScaleDown() == nil {
		// Nothing
	}
}

func (s *Sink) Recycle(step Step) {
	s.pool <- step
}

func (s *Sink) Run(in <-chan interface{}) {
	t1 := time.Now()
	for x := range in {
		if s.cancelled {
			break
		}
		x := x.(Item)
		t2 := time.Now()
		fmt.Printf("%s,%d,Starvation,%d\n", s.id, x.Id, t2.Sub(t1).Microseconds())
		step := <-s.pool
		t3 := time.Now()
		fmt.Printf("%s,%d,Saturation,%d\n", s.id, x.Id, t3.Sub(t2).Microseconds())
		go func(x Item) {
			defer s.Recycle(step)
			_ = step.Exec(x)
		}(x)
		t1 = t3
		fmt.Printf("%s,%d,Duration,%d\n", s.id, x.Id, time.Since(x.Start).Microseconds())
	}
	s.Drain()
}
