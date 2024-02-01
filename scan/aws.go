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
) (*awsClient, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &awsClient{
		config: cfg,
	}, nil
}

func (c *awsClient) AssumeRole(
	ctx context.Context,
	roleARN string,
	externalID string,
) error {
	stsClient := sts.NewFromConfig(c.config)
	credsProvider := stscreds.NewAssumeRoleProvider(
		stsClient,
		roleARN,
		func(o *stscreds.AssumeRoleOptions) {
			o.ExternalID = &externalID
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
	// TODO: Check for next page to see if there are remaining items
	output, err := c.rdsClient.DescribeDBClusters(
		ctx,
		&rds.DescribeDBClustersInput{},
	)
	if err != nil {
		return nil, err
	}
	return output.DBClusters, nil
}

func (c *awsClient) getRDSInstances(
	ctx context.Context,
) ([]rdsType.DBInstance, error) {
	// TODO: Check for next page to see if there are remaining items
	output, err := c.rdsClient.DescribeDBInstances(
		ctx,
		&rds.DescribeDBInstancesInput{},
	)
	if err != nil {
		return nil, err
	}
	return output.DBInstances, nil
}

func (c *awsClient) getRedshiftClusters(
	ctx context.Context,
) ([]rsTypes.Cluster, error) {
	// TODO: Check for next page to see if there are remaining items
	output, err := c.redshiftClient.DescribeClusters(
		ctx,
		&redshift.DescribeClustersInput{},
	)
	if err != nil {
		return nil, err
	}
	return output.Clusters, nil
}

type dynamoDBTable struct {
	Table *ddbTypes.TableDescription
	Tags  []ddbTypes.Tag
}

func (c *awsClient) getDynamoDBTables(
	ctx context.Context,
) ([]dynamoDBTable, error) {
	// TODO: Check for next page to see if there are remaining items
	output, err := c.dynamodbClient.ListTables(
		ctx,
		&dynamodb.ListTablesInput{},
	)
	if err != nil {
		return nil, err
	}
	tables := make([]dynamoDBTable, 0, len(output.TableNames))
	for _, tableName := range output.TableNames {
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
		// TODO: Check for next page to see if there are remaining items
		tagsOutput, err := c.dynamodbClient.ListTagsOfResource(
			ctx,
			&dynamodb.ListTagsOfResourceInput{
				ResourceArn: table.TableArn,
			},
		)
		if err != nil {
			return nil, err
		}
		tables = append(tables, dynamoDBTable{
			Table: table,
			Tags:  tagsOutput.Tags,
		})
	}
	return tables, nil
}

func (c *awsClient) initializeServiceClients() {
	c.rdsClient = rds.NewFromConfig(c.config)
	c.redshiftClient = redshift.NewFromConfig(c.config)
	c.dynamodbClient = dynamodb.NewFromConfig(c.config)
}
