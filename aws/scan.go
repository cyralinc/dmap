package aws

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/scan"
)

type scanFunction func(
	ctx context.Context,
	awsClient *awsClient,
) scanResponse

type scanResponse struct {
	repositories []scan.Repository
	scanErrors   []error
}

func scanRDSClusterRepositories(
	ctx context.Context,
	awsClient *awsClient,
) scanResponse {
	repositories := []scan.Repository{}
	var scanErrors []error
	rdsClusters, err := awsClient.getRDSClusters(ctx)
	if err != nil {
		scanErrors = append(scanErrors, fmt.Errorf(
			"error scanning RDS clusters: %w",
			err,
		))
	}
	for _, cluster := range rdsClusters {
		repositories = append(
			repositories,
			newRepositoryFromRDSCluster(cluster),
		)
	}
	return scanResponse{
		repositories: repositories,
		scanErrors:   scanErrors,
	}
}

func scanRDSInstanceRepositories(
	ctx context.Context,
	awsClient *awsClient,
) scanResponse {
	repositories := []scan.Repository{}
	var scanErrors []error
	rdsInstances, err := awsClient.getRDSInstances(ctx)
	if err != nil {
		scanErrors = append(scanErrors, fmt.Errorf(
			"error scanning RDS instances: %w",
			err,
		))
	}
	for _, instance := range rdsInstances {
		// Skip cluster instances, since they were already added when retrieving
		// the RDS clusters.
		if instance.DBClusterIdentifier == nil {
			repositories = append(
				repositories,
				newRepositoryFromRDSInstance(instance),
			)
		}
	}
	return scanResponse{
		repositories: repositories,
		scanErrors:   scanErrors,
	}
}

func scanRedshiftRepositories(
	ctx context.Context,
	awsClient *awsClient,
) scanResponse {
	repositories := []scan.Repository{}
	var scanErrors []error
	redshiftClusters, err := awsClient.getRedshiftClusters(ctx)
	if err != nil {
		scanErrors = append(scanErrors, fmt.Errorf(
			"error scanning Redshift clusters: %w",
			err,
		))
	}
	for _, cluster := range redshiftClusters {
		repositories = append(
			repositories,
			newRepositoryFromRedshiftCluster(cluster),
		)
	}
	return scanResponse{
		repositories: repositories,
		scanErrors:   scanErrors,
	}
}

func scanDynamoDBRepositories(
	ctx context.Context,
	awsClient *awsClient,
) scanResponse {
	repositories := []scan.Repository{}
	var scanErrors []error
	dynamodbTables, err := awsClient.getDynamoDBTables(ctx)
	if err != nil {
		scanErrors = append(scanErrors, fmt.Errorf(
			"error scanning DynamoDB tables: %w",
			err,
		))
	}
	for _, table := range dynamodbTables {
		repositories = append(
			repositories,
			newRepositoryFromDynamoDBTable(table),
		)
	}
	return scanResponse{
		repositories: repositories,
		scanErrors:   scanErrors,
	}
}
