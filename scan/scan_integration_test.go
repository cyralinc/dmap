//go:build integration

package scan

import (
	"context"
	"fmt"
	"testing"

	"github.com/cyralinc/dmap/config"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ScanManagerIntegrationTestSuite struct {
	suite.Suite
	scanner *ScanManager
}

func (s *ScanManagerIntegrationTestSuite) SetupSuite() {
	ctx := context.Background()
	scanner, err := NewScanManager(ctx, config.Config{
		AWS: &config.AWSConfig{
			Regions: []string{
				"us-east-1",
				"us-east-2",
				"us-west-1",
				"us-west-2",
			},
		},
	})
	require.NoError(s.T(), err)
	s.scanner = scanner
}

func TestIntegrationScanManager(t *testing.T) {
	s := new(ScanManagerIntegrationTestSuite)
	suite.Run(t, s)
}

func (s *ScanManagerIntegrationTestSuite) TestScanRepositories() {
	ctx := context.Background()
	results, scanErrors := s.scanner.ScanRepositories(ctx)
	fmt.Printf("Num. Repositories: %v\n", len(results.Repositories))
	fmt.Printf("Repositories: %v\n", results.Repositories)
	fmt.Printf("Scan Erros: %v\n", scanErrors)
}
