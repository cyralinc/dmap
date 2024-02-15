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
	repositories map[string]scan.Repository
	scanErrors   []error
}

func scanRDSClusterRepositories(
	ctx context.Context,
	awsClient *awsClient,
) scanResponse {
	var scanErrors []error
	rdsClusters, err := awsClient.getRDSClusters(ctx)
	if err != nil {
		scanErrors = append(
			scanErrors,
			fmt.Errorf(
				"error scanning RDS clusters for region %s: %w",
				awsClient.config.Region,
				err,
			),
		)
	}
	repositories := make(map[string]scan.Repository, len(rdsClusters))
	for _, cluster := range rdsClusters {
		repo := newRepositoryFromRDSCluster(cluster)
		repositories[repo.Id] = repo
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
	var scanErrors []error
	rdsInstances, err := awsClient.getRDSInstances(ctx)
	if err != nil {
		scanErrors = append(
			scanErrors,
			fmt.Errorf(
				"error scanning RDS instances for region %s: %w",
				awsClient.config.Region,
				err,
			),
		)
	}
	repositories := make(map[string]scan.Repository, len(rdsInstances))
	for _, instance := range rdsInstances {
		// Skip cluster instances, since they were already added when retrieving
		// the RDS clusters.
		if instance.DBClusterIdentifier == nil {
			repo := newRepositoryFromRDSInstance(instance)
			repositories[repo.Id] = repo
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
	var scanErrors []error
	redshiftClusters, err := awsClient.getRedshiftClusters(ctx)
	if err != nil {
		scanErrors = append(
			scanErrors,
			fmt.Errorf(
				"error scanning Redshift clusters for region %s: %w",
				awsClient.config.Region,
				err,
			),
		)
	}
	repositories := make(map[string]scan.Repository, len(redshiftClusters))
	for _, cluster := range redshiftClusters {
		repo := newRepositoryFromRedshiftCluster(cluster)
		repositories[repo.Id] = repo
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
	var scanErrors []error
	dynamodbTables, err := awsClient.getDynamoDBTables(ctx)
	if err != nil {
		scanErrors = append(
			scanErrors,
			fmt.Errorf(
				"error scanning DynamoDB tables for region %s: %w",
				awsClient.config.Region,
				err,
			),
		)
	}
	repositories := make(map[string]scan.Repository, len(dynamodbTables))
	for _, table := range dynamodbTables {
		repo := newRepositoryFromDynamoDBTable(table)
		repositories[repo.Id] = repo
	}
	return scanResponse{
		repositories: repositories,
		scanErrors:   scanErrors,
	}
}

//lint:ignore U1000 ignored unused for now - WIP
func scanS3Buckets(
	ctx context.Context,
	awsClient *awsClient,
) scanResponse {
	var scanErrors []error
	buckets, err := awsClient.getS3Buckets(ctx)
	if err != nil {
		scanErrors = append(
			scanErrors,
			fmt.Errorf(
				"error scanning S3 buckets for region %s: %w",
				awsClient.config.Region,
				err,
			),
		)
	}
	repos := make(map[string]scan.Repository, len(buckets))
	for _, bucket := range buckets {
		repo := newRepositoryFromS3Bucket(bucket)
		repos[repo.Id] = repo
	}
	return scanResponse{
		repositories: repos,
		scanErrors:   scanErrors,
	}
}
