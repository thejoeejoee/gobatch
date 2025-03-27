package batch

// PipelineStage contains the input and output channels for a single
// stage of the batch pipeline.
type PipelineStage struct {
	// Input contains the input items for a pipeline stage.
	Input <-chan *Item

	// Output is for the output of the pipeline stage.
	Output chan<- *Item

	//Retry chan<- *NewItem

	// Error is for any errors encountered during the pipeline stage.
	Errors chan<- error

	// ids is a channel for generating unique IDs for items, run by Batch.
	ids chan uint64
}

// Close closes the pipeline stage.
//
// Note that it will also close the write channels. Do not close them separately
// or it will panic.
func (p *PipelineStage) Close() {
	close(p.Output)
	close(p.Errors)
}

// NewItem creates a new item for the pipeline stage.
//
// It will assign a unique ID to the item and return it.
// Use for creating entirely new items,
// for converting input items to output items use batch.NextItem.
func (p *PipelineStage) NewItem(data interface{}) *Item {
	return &Item{
		item: data,
		id:   <-p.ids,
	}
}
