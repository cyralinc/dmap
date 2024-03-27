// Package publisher provides types for publishing data classifications results
// to an external source (Publisher), such as to stdout (StdOutPublisher).
package publisher

import (
	"context"

	"github.com/cyralinc/dmap/classification"
)

// Publisher publishes classification and discovery results to some destination,
// which is left up to the implementer.
type Publisher interface {
	// PublishClassifications publishes a slice of
	// classification.ClassifiedTable to some destination. Any error(s) during
	// publication should be returned.
	PublishClassifications(ctx context.Context, repoId string, results []classification.ClassifiedTable) error
}
