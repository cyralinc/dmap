package publisher

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cyralinc/dmap/classification"
)

// StdOutPublisher "publishes" classification results to stdout in JSON format.
type StdOutPublisher struct{}

// StdOutPublisher implements Publisher
var _ Publisher = (*StdOutPublisher)(nil)

func NewStdOutPublisher() *StdOutPublisher {
	return &StdOutPublisher{}
}

func (c *StdOutPublisher) PublishClassifications(
	_ context.Context,
	_ string,
	tables []classification.ClassifiedTable,
) error {
	results := struct {
		Results []classification.ClassifiedTable `json:"results"`
	}{
		Results: tables,
	}
	b, err := json.MarshalIndent(results, "", "    ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}
	fmt.Println(string(b))
	return nil
}
