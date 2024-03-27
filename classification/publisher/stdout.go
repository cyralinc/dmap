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
	repoId string,
	results []classification.Result,
) error {
	classifications := struct {
		Repo            string                  `json:"repo"`
		Classifications []classification.Result `json:"classifications"`
	}{
		Repo:            repoId,
		Classifications: results,
	}
	b, err := json.MarshalIndent(classifications, "", "    ")
	if err != nil {
		return fmt.Errorf("failed to marshal classifications: %w", err)
	}
	fmt.Println(string(b))
	return nil
}
