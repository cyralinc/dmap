package scan

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cyralinc/dmap/aws"
	"github.com/cyralinc/dmap/model"
)

type Scanner struct {
	config    Config
	awsClient *aws.AWSClient
	// Wrap all the errors that happen during the scan.
	scanErrors error
}

func NewScanner(ctx context.Context, config Config) (*Scanner, error) {
	if err := config.validateConfig(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	s := &Scanner{
		config: config,
	}
	if s.config.isAWSConfigured() {
		if err := s.initAWSClient(ctx); err != nil {
			return nil, fmt.Errorf("error initializing AWS client: %w", err)
		}
	} else {
		return nil, fmt.Errorf("AWS configuration must be specified")
	}
	return s, nil
}

func (s *Scanner) ScanRepositories(
	ctx context.Context,
) ([]model.Repository, error) {
	repositories := []model.Repository{}
	if s.config.isAWSConfigured() {
		awsRepos := s.scanAWSRepositories(ctx)
		repositories = append(repositories, awsRepos...)
	}
	return repositories, s.scanErrors
}

func (s *Scanner) initAWSClient(ctx context.Context) error {
	awsClient, err := aws.NewAWSClient(ctx, s.config.AWS.AssumeRole)
	if err != nil {
		return err
	}
	s.awsClient = awsClient
	return nil
}

func (s *Scanner) scanAWSRepositories(ctx context.Context) []model.Repository {
	repositories := []model.Repository{}
	for _, region := range s.config.AWS.Regions {
		s.awsClient.SetRegion(region)

		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		numRoutines := 4
		reposChan := make(chan []model.Repository, numRoutines)
		errorsChan := make(chan error, numRoutines)
		var wg sync.WaitGroup
		wg.Add(numRoutines)

		go aws.ScanRDSClusterRepositories(subCtx, s.awsClient, &wg, reposChan, errorsChan)

		go aws.ScanRDSInstanceRepositories(subCtx, s.awsClient, &wg, reposChan, errorsChan)

		go aws.ScanRedshiftRepositories(subCtx, s.awsClient, &wg, reposChan, errorsChan)

		go aws.ScanDynamoDBRepositories(subCtx, s.awsClient, &wg, reposChan, errorsChan)

		wg.Wait()

		close(reposChan)
		close(errorsChan)

		for repos := range reposChan {
			repositories = append(repositories, repos...)
		}

		for err := range errorsChan {
			s.scanErrors = errors.Join(s.scanErrors, err)
		}
	}
	return repositories
}
