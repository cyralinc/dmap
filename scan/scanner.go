package scan

import (
	"context"
)

type Scanner interface {
	Scan(ctx context.Context) (*ScanResults, error)
}
