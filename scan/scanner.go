package scan

import (
	"context"
	"fmt"
)

type Scanner struct {
	config    Config
	AWSClient *awsClient
}

func NewScanner(ctx context.Context, cfg Config) (*Scanner, error) {
	scanner := &Scanner{
		config: cfg,
	}
	if scanner.config.AWS != nil {
		scanner.initAWSClient(ctx)
	} else {
		return nil, fmt.Errorf("AWS configuration must be specified")
	}
	return scanner, nil
}

func (s *Scanner) initAWSClient(ctx context.Context) error {
	awsClient, err := newAWSClient(ctx)
	if err != nil {
		return err
	}
	s.AWSClient = awsClient
	return nil
}

func (s *Scanner) ScanRepositories(
	ctx context.Context,
) ([]Repository, error) {
	var repositories []Repository
	// Wrap all the errors that happen during the scan.
	var scanErrors error
	for _, region := range s.config.AWS.Regions {
		s.AWSClient.setRegion(region)

		redshiftClusters, err := s.AWSClient.getRedshiftClusters(ctx)
		if err != nil {
			scanErrors = fmt.Errorf(
				"%w: error scanning Redshift clusters: %w",
				scanErrors, err,
			)
		}
		for _, cluster := range redshiftClusters {
			repositories = append(
				repositories,
				newRepositoryFromRedshiftCluster(cluster),
			)
		}

		dynamodbTables, err := s.AWSClient.getDynamoDBTables(ctx)
		if err != nil {
			scanErrors = fmt.Errorf(
				"%w: error scanning dynamodb tables: %w",
				scanErrors, err,
			)
		}
		for _, table := range dynamodbTables {
			repositories = append(
				repositories,
				newRepositoryFromDynamoDBTable(table),
			)
		}

		rdsClusters, err := s.AWSClient.getRDSClusters(ctx)
		if err != nil {
			scanErrors = fmt.Errorf(
				"%w: error scanning RDS clusters: %w",
				scanErrors, err,
			)
		}
		for _, cluster := range rdsClusters {
			repositories = append(
				repositories,
				newRepositoryFromRDSCluster(cluster),
			)
		}

		rdsInstances, err := s.AWSClient.getRDSInstances(ctx)
		if err != nil {
			scanErrors = fmt.Errorf(
				"%w: error scanning RDS instances: %w",
				scanErrors, err,
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
	}
	return repositories, scanErrors
}
