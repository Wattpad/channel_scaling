package pipeline

import (
	"errors"
	"fmt"
	"time"
)

type Stage struct {
	id         string
	pool       chan Step
	numWorkers int
	maxWorkers int
	stepFn     func() Step
	out        chan interface{}
	done       chan struct{}
	cancelled  bool
}

func NewStage(id string, stepFn func() Step) *Stage {
	maxWorkers := 20
	stage := &Stage{
		id:         id,
		pool:       make(chan Step, maxWorkers),
		maxWorkers: maxWorkers,
		stepFn:     stepFn,
		out:        make(chan interface{}),
		done:       make(chan struct{}),
	}
	stage.ScaleUp()
	return stage
}

func (s *Stage) Out() <-chan interface{} {
	return s.out
}

func (s *Stage) Done() <-chan struct{} {
	return s.done
}

// Cancel will close the Done channel. It should only be called by the
// goroutine that created the Stage.
func (s *Stage) Cancel() {
	if !s.cancelled {
		close(s.done)
		s.cancelled = true
	}
}

func (s *Stage) ScaleUp() error {
	if s.numWorkers == s.maxWorkers {
		return errors.New("scale-up limit reached")
	}
	s.pool <- s.stepFn()
	s.numWorkers += 1
	return nil
}

func (s *Stage) ScaleDown() error {
	if s.numWorkers == 0 {
		return errors.New("scale-down limit reached")
	}
	<-s.pool
	s.numWorkers -= 1
	return nil
}

func (s *Stage) Drain() {
	for s.ScaleDown() == nil {
		// Nothing
	}
}

func (s *Stage) Recycle(step Step) {
	s.pool <- step
}

func (s *Stage) Run(in <-chan interface{}, done <-chan struct{}) {
	defer close(s.out)
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
		go func(x Item, t3 time.Time) {
			defer s.Recycle(step)
			y := step.Exec(x)
			t4 := time.Now()
			fmt.Printf("%s,%d,Active,%d\n", s.id, x.Id, t4.Sub(t3).Microseconds())
			select {
			case <-done:
				return
			case s.out <- y:
			}
			t5 := time.Now()
			fmt.Printf("%s,%d,Transmission,%d\n", s.id, x.Id, t5.Sub(t4).Microseconds())
		}(x, t3)
		t1 = t3
	}
	s.Drain()
}
