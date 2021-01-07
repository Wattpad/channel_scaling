package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/tonygoold/channel_scaling/pipeline"
)

func main() {
	if len(os.Args) < 2 {
		scalePipelines(1, 100)
		return
	}
	switch os.Args[1] {
	case "-stage10":
		scaleStages(10, 100)
	case "-stage20":
		scaleStages(20, 100)
	case "-pipeline":
		scalePipelines(10, 100)
	default:
		fmt.Fprintf(os.Stderr, "Usage: [-stage10 | -stage20 | -pipeline]")
		os.Exit(1)
	}
}

func scalePipelines(numPipelines int, limit int) {
	fmt.Printf("Stage,Element,Step,Microseconds\n")

	var wg sync.WaitGroup
	var sources []*pipeline.Source
	for i := 0; i < numPipelines; i++ {
		initial := i
		source := pipeline.NewSource("Stage 0", func() pipeline.Step {
			step := pipeline.NewSourceDelayStep(20 * time.Millisecond)
			step.SetInitial(initial)
			step.SetIncrement(numPipelines)
			return step
		})
		source.SetLimit(limit)
		sources = append(sources, source)
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
		go source.Run(stage1.Done())
		go stage1.Run(source.Out(), stage2.Done())
		go stage2.Run(stage1.Out(), stage3.Done())
		go stage3.Run(stage2.Out(), sink.Done())
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink.Run(stage3.Out())
		}()
	}

	/*
		time.Sleep(3 * time.Second)
		for _, source := range sources {
			source.Cancel()
		}
	*/

	wg.Wait()
}

func scaleStages(maxParallel int, limit int) {
	fmt.Printf("Stage,Element,Step,Microseconds\n")

	source := pipeline.NewSource("Stage 0", func() pipeline.Step {
		return pipeline.NewSourceDelayStep(10 * time.Millisecond)
	})
	source.SetLimit(limit)
	stage1 := pipeline.NewStage("Stage 1", func() pipeline.Step {
		return pipeline.NewDelayStep(100 * time.Millisecond)
	})
	for i := 0; i < 9; i++ {
		stage1.ScaleUp()
	}
	stage2 := pipeline.NewStage("Stage 2", func() pipeline.Step {
		return pipeline.NewDelayStep(200 * time.Millisecond)
	})
	for i := 0; i < maxParallel; i++ {
		stage2.ScaleUp()
	}
	stage3 := pipeline.NewStage("Stage 3", func() pipeline.Step {
		return pipeline.NewDelayStep(10 * time.Millisecond)
	})
	sink := pipeline.NewSink("Stage 4", func() pipeline.Step {
		return pipeline.NewDelayStep(50 * time.Millisecond)
	})
	for i := 0; i < 4; i++ {
		sink.ScaleUp()
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

	/*
		time.Sleep(3 * time.Second)
		source.Cancel()
	*/

	wg.Wait()
}
