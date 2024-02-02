package scan

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsType "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	rsTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type awsClient struct {
	config         aws.Config
	rdsClient      *rds.Client
	redshiftClient *redshift.Client
	dynamodbClient *dynamodb.Client
}

func newAWSClient(
	ctx context.Context,
	assumeRole *AWSAssumeRoleConfig,
) (*awsClient, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	c := &awsClient{
		config: cfg,
	}
	if assumeRole != nil {
		if err := c.assumeRole(ctx, *assumeRole); err != nil {
			return nil, fmt.Errorf("error assuming IAM role: %w", err)
		}
	}
	c.initializeServiceClients()
	return c, nil
}

func (c *awsClient) assumeRole(
	ctx context.Context,
	assumeRole AWSAssumeRoleConfig,
) error {
	stsClient := sts.NewFromConfig(c.config)
	credsProvider := stscreds.NewAssumeRoleProvider(
		stsClient,
		assumeRole.IAMRoleARN,
		func(o *stscreds.AssumeRoleOptions) {
			o.ExternalID = &assumeRole.ExternalID
		},
	)
	c.config.Credentials = aws.NewCredentialsCache(credsProvider)
	// Validate AWS credentials provider.
	if _, err := c.config.Credentials.Retrieve(ctx); err != nil {
		return fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}
	return nil
}

func (c *awsClient) setRegion(region string) {
	c.config.Region = region
	c.initializeServiceClients()
}

func (c *awsClient) getRDSClusters(
	ctx context.Context,
) ([]rdsType.DBCluster, error) {
	var clusters []rdsType.DBCluster
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

func (c *awsClient) getRDSInstances(
	ctx context.Context,
) ([]rdsType.DBInstance, error) {
	var instances []rdsType.DBInstance
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

func (c *awsClient) getRedshiftClusters(
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

func (c *awsClient) getDynamoDBTables(
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
	for _, tableName := range tableNames {
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

func (c *awsClient) initializeServiceClients() {
	c.rdsClient = rds.NewFromConfig(c.config)
	c.redshiftClient = redshift.NewFromConfig(c.config)
	c.dynamodbClient = dynamodb.NewFromConfig(c.config)
}
