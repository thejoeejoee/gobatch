package batch_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MasterOfBinary/gobatch/batch"
	"github.com/MasterOfBinary/gobatch/source"
)

// printProcessor is a Processor that prints items in batches.
//
// To demonstrate how errors can be handled, it fails to process the number 5.
type printProcessor struct{}

// Process prints a batch of items.
func (p printProcessor) Process(ctx context.Context, ps *batch.PipelineStage) {
	// Process needs to close ps after it's done
	defer ps.Close()

	toPrint := make([]interface{}, 0, 5)
	for item := range ps.Input {
		// Get returns the item itself
		if item.Get() == 5 {
			ps.Errors <- errors.New("cannot process 5")
			continue
		}

		toPrint = append(toPrint, item.Get())
	}

	fmt.Println(toPrint)
}

func Example() {
	// Create a batch processor that processes items 5 at a time
	config := batch.NewConstantConfig(&batch.ConfigValues{
		MinItems: 5,
	})
	b := batch.New(config)
	p := &printProcessor{}

	// Channel is a Source that reads from a channel until it's closed
	ch := make(chan interface{})
	s := source.Channel{
		Input: ch,
	}

	// Go runs in the background while the main goroutine processes errors
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := b.Go(ctx, &s, p)

	// Spawn a goroutine that simulates loading data from somewhere
	go func() {
		for i := 0; i < 20; i++ {
			time.Sleep(time.Millisecond * 10)
			ch <- i
		}
		close(ch)
	}()

	// Wait for errors. When the error channel is closed the pipeline has been
	// completely drained. Alternatively, we could wait for Done.
	var lastErr error
	for err := range errs {
		lastErr = err
	}

	fmt.Println("Finished processing.")
	if lastErr != nil {
		fmt.Println("Found error:", lastErr.Error())
	}
	// Output:
	// [0 1 2 3 4]
	// [6 7 8 9]
	// [10 11 12 13 14]
	// [15 16 17 18 19]
	// Finished processing.
	// Found error: cannot process 5
}

// sumProcessor is a Processor that sums items in batches.
type sumProcessor struct{}

// Process prints a batch of items.
func (p *sumProcessor) Process(ctx context.Context, ps *batch.PipelineStage) {
	defer ps.Close()

	out := 0
	for item := range ps.Input {
		out += item.Get().(int)
	}
	ps.Output <- ps.NewItem(out)
}

func Example_sum() {
	// Create a batch processor that processes items 5 at a time
	config := batch.NewConstantConfig(&batch.ConfigValues{
		MinItems: 5,
	})
	b := batch.New(config)
	p := &sumProcessor{}

	// Channel is a Source that reads from a channel until it's closed
	ch := make(chan interface{})
	s := source.Channel{
		Input: ch,
	}

	// Go runs in the background while the main goroutine processes errors
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := b.Go(ctx, &s, p)

	// Spawn a goroutine that simulates loading data from somewhere
	go func() {
		for i := 0; i <= 10; i++ {
			time.Sleep(time.Millisecond * 10)
			ch <- i
		}
		close(ch)
	}()

	// Spawn goroutine to process the output
	go func() {
		for item := range b.Out() {
			fmt.Println("Sum:", item.Get())
		}
	}()

	// Wait for errors. When the error channel is closed the pipeline has been
	// completely drained. Alternatively, we could wait for Done.
	var lastErr error
	for err := range errs {
		lastErr = err
	}

	fmt.Println("Finished processing.")
	if lastErr != nil {
		fmt.Println("Found error:", lastErr.Error())
	}
	// Output:
	// Sum: 10
	// Sum: 35
	// Sum: 10
	// Finished processing.
}
