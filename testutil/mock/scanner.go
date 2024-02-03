package mock

import (
	"context"

	"github.com/cyralinc/dmap/model"
)

type MockScanner struct {
	Repositories []model.Repository
	Err          error
}

func (m *MockScanner) Scan(
	ctx context.Context,
) ([]model.Repository, error) {
	return m.Repositories, m.Err
}
