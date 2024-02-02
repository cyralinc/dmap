package scan

import (
	"context"
	"fmt"
	"sync"
)

type Scanner struct {
	config    Config
	awsClient *awsClient
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
) ([]Repository, error) {
	repositories := []Repository{}
	if s.config.isAWSConfigured() {
		awsRepos := s.scanAWSRepositories(ctx)
		repositories = append(repositories, awsRepos...)
	}
	return repositories, s.scanErrors
}

func (s *Scanner) initAWSClient(ctx context.Context) error {
	awsClient, err := newAWSClient(ctx, s.config.AWS.AssumeRole)
	if err != nil {
		return err
	}
	s.awsClient = awsClient
	return nil
}

func (s *Scanner) scanAWSRepositories(ctx context.Context) []Repository {
	repositories := []Repository{}
	for _, region := range s.config.AWS.Regions {
		s.awsClient.setRegion(region)

		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		numRoutines := 4
		reposChan := make(chan []Repository, numRoutines)
		errorsChan := make(chan error, numRoutines)
		var wg sync.WaitGroup
		wg.Add(numRoutines)

		go scanRDSClusterRepositories(subCtx, s.awsClient, &wg, reposChan, errorsChan)

		go scanRDSInstanceRepositories(subCtx, s.awsClient, &wg, reposChan, errorsChan)

		go scanRedshiftRepositories(subCtx, s.awsClient, &wg, reposChan, errorsChan)

		go scanDynamoDBRepositories(subCtx, s.awsClient, &wg, reposChan, errorsChan)

		wg.Wait()

		close(reposChan)
		close(errorsChan)

		for repos := range reposChan {
			repositories = append(repositories, repos...)
		}

		for err := range errorsChan {
			if err != nil {
				s.appendError(err)
			}
		}
	}
	return repositories
}

func (s *Scanner) appendError(err error) {
	if s.scanErrors == nil {
		s.scanErrors = err
	} else {
		s.scanErrors = fmt.Errorf(
			"%w: %w",
			s.scanErrors, err,
		)
	}
}

func scanRedshiftRepositories(
	ctx context.Context,
	awsClient *awsClient,
	wg *sync.WaitGroup,
	reposChannel chan<- []Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []Repository{}
	var errors error
	redshiftClusters, err := awsClient.getRedshiftClusters(ctx)
	if err != nil {
		errors = fmt.Errorf(
			"error scanning Redshift clusters: %w",
			err,
		)
	}
	for _, cluster := range redshiftClusters {
		repositories = append(
			repositories,
			newRepositoryFromRedshiftCluster(cluster),
		)
	}
	reposChannel <- repositories
	errorsChan <- errors
}

func scanDynamoDBRepositories(
	ctx context.Context,
	awsClient *awsClient,
	wg *sync.WaitGroup,
	reposChannel chan<- []Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []Repository{}
	var errors error
	dynamodbTables, err := awsClient.getDynamoDBTables(ctx)
	if err != nil {
		errors = fmt.Errorf(
			"error scanning DynamoDB tables: %w",
			err,
		)
	}
	for _, table := range dynamodbTables {
		repositories = append(
			repositories,
			newRepositoryFromDynamoDBTable(table),
		)
	}
	reposChannel <- repositories
	errorsChan <- errors
}

func scanRDSClusterRepositories(
	ctx context.Context,
	awsClient *awsClient,
	wg *sync.WaitGroup,
	reposChannel chan<- []Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []Repository{}
	var errors error
	rdsClusters, err := awsClient.getRDSClusters(ctx)
	if err != nil {
		errors = fmt.Errorf(
			"error scanning RDS clusters: %w",
			err,
		)
	}
	for _, cluster := range rdsClusters {
		repositories = append(
			repositories,
			newRepositoryFromRDSCluster(cluster),
		)
	}
	reposChannel <- repositories
	errorsChan <- errors
}

func scanRDSInstanceRepositories(
	ctx context.Context,
	awsClient *awsClient,
	wg *sync.WaitGroup,
	reposChannel chan<- []Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []Repository{}
	var errors error
	rdsInstances, err := awsClient.getRDSInstances(ctx)
	if err != nil {
		errors = fmt.Errorf(
			"error scanning RDS instances: %w",
			err,
		)
	}
	for _, instance := range rdsInstances {
		// Skip cluster instances, since they were already added when retrieving
		// the RDS clusters.
		if instance.DBClusterIdentifier != nil {
			continue
		}
		repositories = append(
			repositories,
			newRepositoryFromRDSInstance(instance),
		)
	}
	reposChannel <- repositories
	errorsChan <- errors
}
