package classification

import (
	"context"
)

// Publisher publishes classification and discovery results to some destination,
// which is left up to the implementer.
// TODO: add doc about labels -ccampo 2024-04-02
type Publisher interface {
	// PublishClassifications publishes a slice of ClassifiedTable to some
	// destination. Any error(s) during publication should be returned.
	// TODO: add labels -ccampo 2024-04-02
	PublishClassifications(ctx context.Context, repoId string, results []ClassifiedTable) error
}
