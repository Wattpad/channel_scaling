package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/tonygoold/channel_scaling/pipeline"
)

type Tuning bool

const (
	Untuned = false
	Tuned   = true
)

func main() {
	var stage bool
	var tuned bool
	var pipeline bool
	var concurrency int
	var numTasks int

	flag.BoolVar(&stage, "stage", false, "Apply concurrency at the stage level (default false)")
	flag.BoolVar(&tuned, "tuned", false, "Use tuned stage level concurrency (default false)")
	flag.BoolVar(&pipeline, "pipeline", false, "Apply concurrency at the pipeline level (default false)")
	flag.IntVar(&concurrency, "concurrency", 1, "Maximum concurrency (default 1)")
	flag.IntVar(&numTasks, "tasks", 100, "Number of tasks to run (default 100)")
	flag.Parse()

	if stage && pipeline {
		panic("Specify either -stage or -pipeline but not both")
	}

	start := time.Now()
	if stage {
		if tuned {
			scaleStages(concurrency, numTasks, Tuned)
		} else {
			scaleStages(concurrency, numTasks, Untuned)
		}
	} else if pipeline {
		scalePipelines(concurrency, numTasks)
	} else {
		scaleTransactions(concurrency, numTasks)
	}
	duration := time.Since(start)
	throughput := float64(numTasks) / duration.Seconds()
	fmt.Fprintf(os.Stderr, "Throughput: %0.1f/s\n", throughput)
}

func scaleTransactions(numTransactions int, limit int) {
	fmt.Printf("Stage,Element,Step,Microseconds\n")

	source := pipeline.NewSource("Stage 0", func() pipeline.Step { return pipeline.NewSourceStep() })
	source.SetLimit(limit)

	var wg sync.WaitGroup
	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// To level the playing field with pipelines, generate a new task as soon as one is dequeued.
			buf := make(chan interface{})
			go func() {
				defer close(buf)
				for x := range source.Out() {
					buf <- x
				}
			}()

			step1 := pipeline.NewDelayStep(100 * time.Millisecond)
			step2 := pipeline.NewDelayStep(200 * time.Millisecond)
			step3 := pipeline.NewDelayStep(10 * time.Millisecond)
			sink := pipeline.NewDelayStep(50 * time.Millisecond)
			for x := range buf {
				sink.Exec(step3.Exec(step2.Exec(step1.Exec(x))))
				x := x.(pipeline.Item)
				fmt.Printf("Stage 4,%d,Duration,%d\n", x.Id, time.Since(x.Start).Microseconds())
			}
		}()
	}

	done := make(chan struct{})
	defer close(done)
	go source.Run(done)

	wg.Wait()
}

func scalePipelines(maxConcurrency int, limit int) {
	stepConcurrency := 5
	if maxConcurrency < stepConcurrency {
		panic("Insufficient concurrency to start a pipeline")
	}

	fmt.Printf("Stage,Element,Step,Microseconds\n")

	source := pipeline.NewSource("Stage 0", func() pipeline.Step {
		return pipeline.NewSourceStep()
	})
	source.SetLimit(limit)

	var wg sync.WaitGroup
	for concurrency := 0; concurrency+stepConcurrency <= maxConcurrency; concurrency += stepConcurrency {
		stage1 := pipeline.NewStage("Stage 1", func() pipeline.Step {
			return pipeline.NewDelayStep(100 * time.Millisecond)
		})
		stage2 := pipeline.NewStage("Stage 2", func() pipeline.Step {
			return pipeline.NewDelayStep(200 * time.Millisecond)
		})
		stage3 := pipeline.NewStage("Stage 3", func() pipeline.Step {
			return pipeline.NewDelayStep(10 * time.Millisecond)
		})
		sink := pipeline.NewSink("Stage 4", func() pipeline.Step {
			return pipeline.NewDelayStep(50 * time.Millisecond)
		})
		go stage1.Run(source.Out(), stage2.Done())
		go stage2.Run(stage1.Out(), stage3.Done())
		go stage3.Run(stage2.Out(), sink.Done())
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink.Run(stage3.Out())
		}()
	}

	done := make(chan struct{})
	defer close(done)
	go source.Run(done)

	wg.Wait()
}

func scaleStages(maxConcurrency int, limit int, tuning Tuning) {
	concurrency := 5
	if maxConcurrency < concurrency {
		panic("Insufficient concurrency to start a pipeline")
	}

	fmt.Printf("Stage,Element,Step,Microseconds\n")

	source := pipeline.NewSource("Stage 0", func() pipeline.Step { return pipeline.NewSourceStep() })
	source.SetLimit(limit)

	stage1 := pipeline.NewStage("Stage 1", func() pipeline.Step {
		return pipeline.NewDelayStep(100 * time.Millisecond)
	})
	stage2 := pipeline.NewStage("Stage 2", func() pipeline.Step {
		return pipeline.NewDelayStep(200 * time.Millisecond)
	})
	stage3 := pipeline.NewStage("Stage 3", func() pipeline.Step {
		return pipeline.NewDelayStep(10 * time.Millisecond)
	})
	sink := pipeline.NewSink("Stage 4", func() pipeline.Step {
		return pipeline.NewDelayStep(50 * time.Millisecond)
	})

	scaleFactors := []int{2, 1, 2, 2}
	if tuning == Tuned {
		scaleFactors = []int{2, 1, 20, 4}
	}
	scalings := []int{1, 1, 1, 1}
	for concurrency < maxConcurrency {
		concurrency++
		throughputs := []int{
			scalings[0] * scaleFactors[0],
			scalings[1] * scaleFactors[1],
			scalings[2] * scaleFactors[2],
			scalings[3] * scaleFactors[3],
		}
		min := throughputs[0]
		if throughputs[1] < min {
			min = throughputs[1]
		}
		if throughputs[2] < min {
			min = throughputs[2]
		}
		if throughputs[3] < min {
			min = throughputs[3]
		}
		if throughputs[0] == min {
			stage1.ScaleUp()
			scalings[0] += 1
		} else if throughputs[1] == min {
			stage2.ScaleUp()
			scalings[1] += 1
		} else if throughputs[2] == min {
			stage3.ScaleUp()
			scalings[2] += 1
		} else {
			sink.ScaleUp()
			scalings[3] += 1
		}
	}

	go source.Run(stage1.Done())
	go stage1.Run(source.Out(), stage2.Done())
	go stage2.Run(stage1.Out(), stage3.Done())
	go stage3.Run(stage2.Out(), sink.Done())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sink.Run(stage3.Out())
	}()

	wg.Wait()
}
