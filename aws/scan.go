package aws

import (
	"context"
	"fmt"
	"sync"

	"github.com/cyralinc/dmap/model"
)

func ScanRedshiftRepositories(
	ctx context.Context,
	awsClient *AWSClient,
	wg *sync.WaitGroup,
	reposChannel chan<- []model.Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []model.Repository{}
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

func ScanDynamoDBRepositories(
	ctx context.Context,
	awsClient *AWSClient,
	wg *sync.WaitGroup,
	reposChannel chan<- []model.Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []model.Repository{}
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

func ScanRDSClusterRepositories(
	ctx context.Context,
	awsClient *AWSClient,
	wg *sync.WaitGroup,
	reposChannel chan<- []model.Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []model.Repository{}
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

func ScanRDSInstanceRepositories(
	ctx context.Context,
	awsClient *AWSClient,
	wg *sync.WaitGroup,
	reposChannel chan<- []model.Repository,
	errorsChan chan<- error,
) {
	defer wg.Done()
	repositories := []model.Repository{}
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
