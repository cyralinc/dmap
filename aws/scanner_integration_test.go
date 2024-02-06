//go:build integration

package aws

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type AWSScannerIntegrationTestSuite struct {
	suite.Suite
	scanner *AWSScanner
}

func (s *AWSScannerIntegrationTestSuite) SetupSuite() {
	ctx := context.Background()
	scanner, err := NewAWSScanner(ctx, ScannerConfig{
		Regions: []string{
			"us-east-1",
			"us-east-2",
			"us-west-1",
			"us-west-2",
		},
	})
	require.NoError(s.T(), err)
	s.scanner = scanner
}

func TestIntegrationAWSScanner(t *testing.T) {
	s := new(AWSScannerIntegrationTestSuite)
	suite.Run(t, s)
}

func (s *AWSScannerIntegrationTestSuite) TestScan() {
	ctx := context.Background()
	results, scanErrors := s.scanner.Scan(ctx)
	fmt.Printf("Num. Repositories: %v\n", len(results.Repositories))
	fmt.Printf("Repositories: %v\n", results.Repositories)
	fmt.Printf("Scan Erros: %v\n", scanErrors)
}
