package mock

import (
	"context"

	"github.com/cyralinc/dmap/scan"
)

type MockScanner struct {
	Repositories []scan.Repository
	Err          error
}

func (m *MockScanner) Scan(
	ctx context.Context,
) ([]scan.Repository, error) {
	return m.Repositories, m.Err
}
