package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	rsTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cyralinc/dmap/config"
	"github.com/cyralinc/dmap/model"
)

type AWSScanner struct {
	scanConfig     config.AWSConfig
	awsConfig      aws.Config
	rdsClient      *rds.Client
	redshiftClient *redshift.Client
	dynamodbClient *dynamodb.Client
}

func NewAWSScanner(
	ctx context.Context,
	scanConfig config.AWSConfig,
) (*AWSScanner, error) {
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	s := &AWSScanner{
		scanConfig: scanConfig,
		awsConfig:  awsConfig,
	}
	if s.scanConfig.AssumeRole != nil {
		if err := s.assumeRole(ctx); err != nil {
			return nil, fmt.Errorf("error assuming IAM role: %w", err)
		}
	}
	s.initializeServiceClients()
	return s, nil
}

func (s *AWSScanner) Scan(ctx context.Context) ([]model.Repository, error) {
	repositories := []model.Repository{}
	var scanErrors []error
	for _, region := range s.scanConfig.Regions {
		s.setRegion(region)

		scanFunctions := []ScanFunction{
			scanRDSClusterRepositories,
			scanRDSInstanceRepositories,
			scanRedshiftRepositories,
			scanDynamoDBRepositories,
		}

		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		numRoutines := len(scanFunctions)
		reposChan := make(chan []model.Repository, numRoutines)
		errorsChan := make(chan error, numRoutines)
		var wg sync.WaitGroup
		wg.Add(numRoutines)

		for i := range scanFunctions {
			go func(scanFunc ScanFunction) {
				defer wg.Done()
				repositories, errors := scanFunc(subCtx, s)
				reposChan <- repositories
				errorsChan <- errors
			}(scanFunctions[i])
		}

		wg.Wait()

		close(reposChan)
		close(errorsChan)

		for repos := range reposChan {
			repositories = append(repositories, repos...)
		}

		for err := range errorsChan {
			scanErrors = append(scanErrors, err)
		}
	}
	return repositories, errors.Join(scanErrors...)
}

func (c *AWSScanner) setRegion(region string) {
	c.awsConfig.Region = region
	c.initializeServiceClients()
}

func (s *AWSScanner) assumeRole(
	ctx context.Context,
) error {
	stsClient := sts.NewFromConfig(s.awsConfig)
	credsProvider := stscreds.NewAssumeRoleProvider(
		stsClient,
		s.scanConfig.AssumeRole.IAMRoleARN,
		func(o *stscreds.AssumeRoleOptions) {
			o.ExternalID = &s.scanConfig.AssumeRole.ExternalID
		},
	)
	s.awsConfig.Credentials = aws.NewCredentialsCache(credsProvider)
	// Validate AWS credentials provider.
	if _, err := s.awsConfig.Credentials.Retrieve(ctx); err != nil {
		return fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}
	return nil
}

func (c *AWSScanner) initializeServiceClients() {
	c.rdsClient = rds.NewFromConfig(c.awsConfig)
	c.redshiftClient = redshift.NewFromConfig(c.awsConfig)
	c.dynamodbClient = dynamodb.NewFromConfig(c.awsConfig)
}

func (c *AWSScanner) getRDSClusters(
	ctx context.Context,
) ([]rdsTypes.DBCluster, error) {
	var clusters []rdsTypes.DBCluster
	// Used for pagination
	var marker *string
	for {
		output, err := c.rdsClient.DescribeDBClusters(
			ctx,
			&rds.DescribeDBClustersInput{
				Marker: marker,
			},
		)
		if err != nil {
			return nil, err
		}

		clusters = append(clusters, output.DBClusters...)

		if output.Marker == nil {
			break
		} else {
			marker = output.Marker
		}
	}
	return clusters, nil
}

func (c *AWSScanner) getRDSInstances(
	ctx context.Context,
) ([]rdsTypes.DBInstance, error) {
	var instances []rdsTypes.DBInstance
	// Used for pagination
	var marker *string
	for {
		output, err := c.rdsClient.DescribeDBInstances(
			ctx,
			&rds.DescribeDBInstancesInput{
				Marker: marker,
			},
		)
		if err != nil {
			return nil, err
		}

		instances = append(instances, output.DBInstances...)

		if output.Marker == nil {
			break
		} else {
			marker = output.Marker
		}
	}
	return instances, nil
}

func (c *AWSScanner) getRedshiftClusters(
	ctx context.Context,
) ([]rsTypes.Cluster, error) {
	var clusters []rsTypes.Cluster
	// Used for pagination
	var marker *string
	for {
		output, err := c.redshiftClient.DescribeClusters(
			ctx,
			&redshift.DescribeClustersInput{
				Marker: marker,
			},
		)
		if err != nil {
			return nil, err
		}

		clusters = append(clusters, output.Clusters...)

		if output.Marker == nil {
			break
		} else {
			marker = output.Marker
		}
	}
	return clusters, nil
}

type dynamoDBTable struct {
	Table *ddbTypes.TableDescription
	Tags  []ddbTypes.Tag
}

func (c *AWSScanner) getDynamoDBTables(
	ctx context.Context,
) ([]dynamoDBTable, error) {
	var tableNames []string
	// Used for pagination
	var exclusiveStartTableName *string
	for {
		output, err := c.dynamodbClient.ListTables(
			ctx,
			&dynamodb.ListTablesInput{
				ExclusiveStartTableName: exclusiveStartTableName,
			},
		)
		if err != nil {
			return nil, err
		}

		tableNames = append(tableNames, output.TableNames...)

		if output.LastEvaluatedTableName == nil {
			break
		} else {
			exclusiveStartTableName = output.LastEvaluatedTableName
		}
	}

	tables := make([]dynamoDBTable, 0, len(tableNames))
	for i := range tableNames {
		tableName := tableNames[i]
		describeTableOutput, err := c.dynamodbClient.DescribeTable(
			ctx,
			&dynamodb.DescribeTableInput{
				TableName: &tableName,
			},
		)
		if err != nil {
			return nil, err
		}
		table := describeTableOutput.Table

		var tableTags []ddbTypes.Tag
		// Used for pagination
		var nextToken *string
		for {
			tagsOutput, err := c.dynamodbClient.ListTagsOfResource(
				ctx,
				&dynamodb.ListTagsOfResourceInput{
					ResourceArn: table.TableArn,
					NextToken:   nextToken,
				},
			)
			if err != nil {
				return nil, err
			}

			tableTags = append(tableTags, tagsOutput.Tags...)

			if tagsOutput.NextToken == nil {
				break
			} else {
				nextToken = tagsOutput.NextToken
			}
		}

		tables = append(tables, dynamoDBTable{
			Table: table,
			Tags:  tableTags,
		})
	}
	return tables, nil
}
