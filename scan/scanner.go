package scan

import (
	"context"
)

// Scanner is an interface that should be implemented for a specific cloud
// provider (e.g. AWS, GCP, etc). It defines the Scan method responsible for
// scanning the existing data repositories of the corresponding cloud provider
// environment.
type Scanner interface {
	Scan(ctx context.Context) (*ScanResults, error)
}
