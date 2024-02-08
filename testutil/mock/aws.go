package mock

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/docdb"
	docdbTypes "github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	redshiftTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
)

type MockRDSClient struct {
	DBClusters  []rdsTypes.DBCluster
	DBInstances []rdsTypes.DBInstance
	Errors      map[string]error
}

func (m *MockRDSClient) DescribeDBClusters(
	ctx context.Context,
	params *rds.DescribeDBClustersInput,
	optFns ...func(*rds.Options),
) (*rds.DescribeDBClustersOutput, error) {
	if m.Errors["DescribeDBClusters"] != nil {
		return nil, m.Errors["DescribeDBClusters"]
	}
	if params.Marker == nil {
		return &rds.DescribeDBClustersOutput{
			DBClusters: []rdsTypes.DBCluster{
				m.DBClusters[0],
				m.DBClusters[1],
			},
			Marker: aws.String("2"),
		}, nil
	}
	return &rds.DescribeDBClustersOutput{
		DBClusters: []rdsTypes.DBCluster{
			m.DBClusters[2],
		},
	}, nil
}

func (m *MockRDSClient) DescribeDBInstances(
	ctx context.Context,
	params *rds.DescribeDBInstancesInput,
	optFns ...func(*rds.Options),
) (*rds.DescribeDBInstancesOutput, error) {
	if m.Errors["DescribeDBInstances"] != nil {
		return nil, m.Errors["DescribeDBInstances"]
	}
	if params.Marker == nil {
		return &rds.DescribeDBInstancesOutput{
			DBInstances: []rdsTypes.DBInstance{
				m.DBInstances[0],
				m.DBInstances[1],
			},
			Marker: aws.String("2"),
		}, nil
	}
	return &rds.DescribeDBInstancesOutput{
		DBInstances: []rdsTypes.DBInstance{
			m.DBInstances[2],
		},
	}, nil
}

type MockDocumentDBClient struct {
	Clusters  []docdbTypes.DBCluster
	Instances []docdbTypes.DBInstance
	Tags      []docdbTypes.Tag
	Errors    map[string]error
}

func (m *MockDocumentDBClient) DescribeDBClusters(
	ctx context.Context,
	params *docdb.DescribeDBClustersInput,
	optFns ...func(*docdb.Options),
) (*docdb.DescribeDBClustersOutput, error) {

	if m.Errors["DescribeDBClusters"] != nil {
		return nil, m.Errors["DescribeDBClusters"]
	}

	if params.Marker == nil {
		return &docdb.DescribeDBClustersOutput{
			DBClusters: []docdbTypes.DBCluster{
				m.Clusters[0],
				m.Clusters[1],
			},
			Marker: aws.String("2"),
		}, nil
	}

	return &docdb.DescribeDBClustersOutput{
		DBClusters: []docdbTypes.DBCluster{
			m.Clusters[2],
		},
	}, nil
}

func (m *MockDocumentDBClient) DescribeDBInstances(
	ctx context.Context,
	params *docdb.DescribeDBInstancesInput,
	optFns ...func(*docdb.Options),
) (*docdb.DescribeDBInstancesOutput, error) {
	if m.Errors["DescribeDBInstances"] != nil {
		return nil, m.Errors["DescribeDBInstances"]
	}

	if params.Marker == nil {
		return &docdb.DescribeDBInstancesOutput{
			DBInstances: []docdbTypes.DBInstance{
				m.Instances[0],
				m.Instances[1],
			},
			Marker: aws.String("2"),
		}, nil
	}

	return &docdb.DescribeDBInstancesOutput{
		DBInstances: []docdbTypes.DBInstance{
			m.Instances[2],
		},
	}, nil
}

func (m *MockDocumentDBClient) ListTagsForResource(
	ctx context.Context,
	params *docdb.ListTagsForResourceInput,
	optFns ...func(*docdb.Options),
) (*docdb.ListTagsForResourceOutput, error) {
	if m.Errors["ListTagsForResource"] != nil {
		return nil, m.Errors["ListTagsForResource"]
	}

	return &docdb.ListTagsForResourceOutput{
		TagList: m.Tags,
	}, nil
}

type MockRedshiftClient struct {
	Clusters []redshiftTypes.Cluster
	Errors   map[string]error
}

func (m *MockRedshiftClient) DescribeClusters(
	ctx context.Context,
	params *redshift.DescribeClustersInput,
	optFns ...func(*redshift.Options),
) (*redshift.DescribeClustersOutput, error) {
	if m.Errors["DescribeClusters"] != nil {
		return nil, m.Errors["DescribeClusters"]
	}
	if params.Marker == nil {
		return &redshift.DescribeClustersOutput{
			Clusters: []redshiftTypes.Cluster{
				m.Clusters[0],
				m.Clusters[1],
			},
			Marker: aws.String("2"),
		}, nil
	}
	return &redshift.DescribeClustersOutput{
		Clusters: []redshiftTypes.Cluster{
			m.Clusters[2],
		},
	}, nil
}

type MockDynamoDBClient struct {
	TableNames []string
	Table      map[string]*dynamodbTypes.TableDescription
	Tags       []dynamodbTypes.Tag
	Errors     map[string]error
}

func (m *MockDynamoDBClient) ListTables(
	ctx context.Context,
	params *dynamodb.ListTablesInput,
	optFns ...func(*dynamodb.Options),
) (*dynamodb.ListTablesOutput, error) {
	if m.Errors["ListTables"] != nil {
		return nil, m.Errors["ListTables"]
	}
	if params.ExclusiveStartTableName == nil {
		return &dynamodb.ListTablesOutput{
			TableNames: []string{
				m.TableNames[0],
				m.TableNames[1],
			},
			LastEvaluatedTableName: aws.String(m.TableNames[1]),
		}, nil
	}
	return &dynamodb.ListTablesOutput{
		TableNames: []string{
			m.TableNames[2],
		},
	}, nil
}

func (m *MockDynamoDBClient) DescribeTable(
	ctx context.Context,
	params *dynamodb.DescribeTableInput,
	optFns ...func(*dynamodb.Options),
) (*dynamodb.DescribeTableOutput, error) {
	if m.Errors["DescribeTable"] != nil {
		return nil, m.Errors["DescribeTable"]
	}
	return &dynamodb.DescribeTableOutput{
		Table: m.Table[*params.TableName],
	}, nil
}

func (m *MockDynamoDBClient) ListTagsOfResource(
	ctx context.Context,
	params *dynamodb.ListTagsOfResourceInput,
	optFns ...func(*dynamodb.Options),
) (*dynamodb.ListTagsOfResourceOutput, error) {
	if m.Errors["ListTagsOfResource"] != nil {
		return nil, m.Errors["ListTagsOfResource"]
	}
	if params.NextToken == nil {
		return &dynamodb.ListTagsOfResourceOutput{
			Tags: []dynamodbTypes.Tag{
				m.Tags[0],
				m.Tags[1],
			},
			NextToken: aws.String("2"),
		}, nil
	}
	return &dynamodb.ListTagsOfResourceOutput{
		Tags: []dynamodbTypes.Tag{
			m.Tags[2],
		},
	}, nil
}
