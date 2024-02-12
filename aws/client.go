package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/docdb"
	docdbTypes "github.com/aws/aws-sdk-go-v2/service/docdb/types"
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

type documentDBClient interface {
	DescribeDBClusters(
		ctx context.Context,
		params *docdb.DescribeDBClustersInput,
		optFns ...func(*docdb.Options),
	) (*docdb.DescribeDBClustersOutput, error)

	ListTagsForResource(
		ctx context.Context,
		params *docdb.ListTagsForResourceInput,
		optFns ...func(*docdb.Options),
	) (*docdb.ListTagsForResourceOutput, error)
}

type awsClient struct {
	config   aws.Config
	rds      rdsClient
	redshift redshiftClient
	dynamodb dynamoDBClient
	docdb    documentDBClient
}

type awsClientConstructor func(awsConfig aws.Config) *awsClient

func newAWSClient(awsConfig aws.Config) *awsClient {
	return &awsClient{
		config:   awsConfig,
		rds:      rds.NewFromConfig(awsConfig),
		redshift: redshift.NewFromConfig(awsConfig),
		dynamodb: dynamodb.NewFromConfig(awsConfig),
		docdb:    docdb.NewFromConfig(awsConfig),
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

type docdbCluster struct {
	cluster docdbTypes.DBCluster
	tags    []string
}

func (c *awsClient) getDocumentDBClusters(
	ctx context.Context,
) ([]docdbCluster, error) {
	// First we need to fetch all clusters. These have a bunch of information, but
	// not all that we need.
	clusters := []docdbTypes.DBCluster{}
	var marker *string // Used for pagination
	for {
		output, err := c.docdb.DescribeDBClusters(
			ctx,
			&docdb.DescribeDBClustersInput{
				Filters: []docdbTypes.Filter{
					{
						Name:   aws.String("engine"),
						Values: []string{"docdb"},
					},
				},
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

	// OK, we now have all the clusters. We can iterate through them, fetching
	// all their tags

	// Map from cluster ARN to all the cluster and instance tags
	tags := make(map[string][]string, len(clusters))
	for i := range clusters {
		clusterARN := clusters[i].DBClusterArn
		output, err := c.docdb.ListTagsForResource(
			ctx,
			&docdb.ListTagsForResourceInput{
				ResourceName: clusters[i].DBClusterArn,
			},
		)
		if err != nil {
			return nil, err
		}

		formattedTags := make([]string, len(output.TagList))
		for i, tag := range output.TagList {
			formattedTags[i] = formatTag(tag.Key, tag.Value)
		}

		tags[*clusterARN] = formattedTags
	}

	// Phew, that was a lot of work, but we have all that we wanted:
	// All clusters in the <clusters> variable
	// A map from cluster ARN to instances, in the <instances> variable
	// A map from cluster ARN to tags, in the <tags> variable
	ret := make([]docdbCluster, len(tags))
	for i := range clusters {
		clusterARN := clusters[i].DBClusterArn
		clusterTags := tags[*clusterARN]

		ret[i] = docdbCluster{
			cluster: clusters[i],
			tags:    clusterTags,
		}
	}

	return ret, nil
}
