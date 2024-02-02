package scan

import (
	"context"
	"errors"
	"fmt"

	"github.com/cyralinc/dmap/aws"
	"github.com/cyralinc/dmap/config"
	"github.com/cyralinc/dmap/model"
)

type Scanner interface {
	Scan(ctx context.Context) ([]model.Repository, error)
}

type ScanManager struct {
	config   config.Config
	scanners []Scanner
}

type ScanResults struct {
	Repositories []model.Repository
}

func NewScanManager(ctx context.Context, config config.Config) (*ScanManager, error) {
	if err := config.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	s := &ScanManager{
		config: config,
	}
	if err := s.initAWSScanner(ctx); err != nil {
		return nil, fmt.Errorf("error initializing AWS scanner: %w", err)
	}
	return s, nil
}

func (s *ScanManager) ScanRepositories(
	ctx context.Context,
) (*ScanResults, error) {
	results := &ScanResults{
		Repositories: []model.Repository{},
	}
	var scanErrors error
	for _, scanner := range s.scanners {
		repos, err := scanner.Scan(ctx)
		if err != nil {
			scanErrors = errors.Join(scanErrors, err)
		}
		results.Repositories = append(results.Repositories, repos...)
	}
	return results, scanErrors
}

func (s *ScanManager) initAWSScanner(ctx context.Context) error {
	awsScanner, err := aws.NewAWSScanner(ctx, *s.config.AWS)
	if err != nil {
		return err
	}
	s.scanners = append(s.scanners, Scanner(awsScanner))
	return nil
}
