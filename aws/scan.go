package aws

import (
	"context"
	"fmt"
	"sync"

	"github.com/cyralinc/dmap/model"
)

func scanRedshiftRepositories(
	ctx context.Context,
	scanner *AWSScanner,
	wg *sync.WaitGroup,
	reposChannel chan<- []model.Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []model.Repository{}
	var errors error
	redshiftClusters, err := scanner.getRedshiftClusters(ctx)
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
	scanner *AWSScanner,
	wg *sync.WaitGroup,
	reposChannel chan<- []model.Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []model.Repository{}
	var errors error
	dynamodbTables, err := scanner.getDynamoDBTables(ctx)
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
	scanner *AWSScanner,
	wg *sync.WaitGroup,
	reposChannel chan<- []model.Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []model.Repository{}
	var errors error
	rdsClusters, err := scanner.getRDSClusters(ctx)
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
	scanner *AWSScanner,
	wg *sync.WaitGroup,
	reposChannel chan<- []model.Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []model.Repository{}
	var errors error
	rdsInstances, err := scanner.getRDSInstances(ctx)
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
