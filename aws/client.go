package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	rsTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
)

type rdsClient interface {
	DescribeDBClusters(
		ctx context.Context,
		params *rds.DescribeDBClustersInput,
		optFns ...func(*rds.Options),
	) (*rds.DescribeDBClustersOutput, error)
	DescribeDBInstances(
		ctx context.Context,
		params *rds.DescribeDBInstancesInput,
		optFns ...func(*rds.Options),
	) (*rds.DescribeDBInstancesOutput, error)
}

type redshiftClient interface {
	DescribeClusters(
		ctx context.Context,
		params *redshift.DescribeClustersInput,
		optFns ...func(*redshift.Options),
	) (*redshift.DescribeClustersOutput, error)
}

type dynamoDBClient interface {
	ListTables(
		ctx context.Context,
		params *dynamodb.ListTablesInput,
		optFns ...func(*dynamodb.Options),
	) (*dynamodb.ListTablesOutput, error)
	DescribeTable(
		ctx context.Context,
		params *dynamodb.DescribeTableInput,
		optFns ...func(*dynamodb.Options),
	) (*dynamodb.DescribeTableOutput, error)
	ListTagsOfResource(
		ctx context.Context,
		params *dynamodb.ListTagsOfResourceInput,
		optFns ...func(*dynamodb.Options),
	) (*dynamodb.ListTagsOfResourceOutput, error)
}

type awsClient struct {
	config   aws.Config
	rds      rdsClient
	redshift redshiftClient
	dynamodb dynamoDBClient
}

type awsClientConstructor func(awsConfig aws.Config) *awsClient

func newAWSClient(awsConfig aws.Config) *awsClient {
	return &awsClient{
		config:   awsConfig,
		rds:      rds.NewFromConfig(awsConfig),
		redshift: redshift.NewFromConfig(awsConfig),
		dynamodb: dynamodb.NewFromConfig(awsConfig),
	}
}

func (c *awsClient) getRDSClusters(
	ctx context.Context,
) ([]rdsTypes.DBCluster, error) {
	var clusters []rdsTypes.DBCluster
	// Used for pagination
	var marker *string
	for {
		output, err := c.rds.DescribeDBClusters(
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
) ([]rdsTypes.DBInstance, error) {
	var instances []rdsTypes.DBInstance
	// Used for pagination
	var marker *string
	for {
		output, err := c.rds.DescribeDBInstances(
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
		output, err := c.redshift.DescribeClusters(
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
	Table ddbTypes.TableDescription
	Tags  []ddbTypes.Tag
}

func (c *awsClient) getDynamoDBTables(
	ctx context.Context,
) ([]dynamoDBTable, error) {
	var tableNames []string
	// Used for pagination
	var exclusiveStartTableName *string
	for {
		output, err := c.dynamodb.ListTables(
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
		describeTableOutput, err := c.dynamodb.DescribeTable(
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
			tagsOutput, err := c.dynamodb.ListTagsOfResource(
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
			Table: *table,
			Tags:  tableTags,
		})
	}
	return tables, nil
}
