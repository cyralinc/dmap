package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dynamodbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	redshiftTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cyralinc/dmap/scan"
)

type AWSScannerTestSuite struct {
	suite.Suite

	dummyRDSClusters  []rdsTypes.DBCluster
	dummyRDSInstances []rdsTypes.DBInstance

	dummyRedshiftClusters []redshiftTypes.Cluster

	dummyDynamoDBTableNames []string
	dummyDynamoDBTable      map[string]*types.TableDescription
	dummyDynamoDBTags       []types.Tag

	dummyS3Buckets []s3Types.Bucket
	dummyS3Tags    []s3Types.Tag
}

func (s *AWSScannerTestSuite) SetupSuite() {
	s.dummyRDSClusters = []rdsTypes.DBCluster{
		{
			DBClusterArn:        aws.String("dummy-rds-cluster-arn-1"),
			DBClusterIdentifier: aws.String("rds-cluster-1"),
		},
		{
			DBClusterArn:        aws.String("dummy-rds-cluster-arn-2"),
			DBClusterIdentifier: aws.String("rds-cluster-2"),
		},
		{
			DBClusterArn:        aws.String("dummy-rds-cluster-arn-3"),
			DBClusterIdentifier: aws.String("rds-cluster-3"),
		},
	}
	s.dummyRDSInstances = []rdsTypes.DBInstance{
		{
			DBInstanceArn:        aws.String("dummy-rds-instance-arn-1"),
			DBInstanceIdentifier: aws.String("rds-instance-1"),
		},
		{
			DBInstanceArn:        aws.String("dummy-rds-instance-arn-2"),
			DBInstanceIdentifier: aws.String("rds-instance-2"),
		},
		{
			DBInstanceArn:        aws.String("dummy-rds-instance-arn-3"),
			DBInstanceIdentifier: aws.String("rds-instance-3"),
		},
	}
	s.dummyRedshiftClusters = []redshiftTypes.Cluster{
		{
			ClusterNamespaceArn: aws.String("dummy-redshift-cluster-arn-1"),
			ClusterIdentifier:   aws.String("redshift-cluster-1"),
		},
		{
			ClusterNamespaceArn: aws.String("dummy-redshift-cluster-arn-2"),
			ClusterIdentifier:   aws.String("redshift-cluster-2"),
		},
		{
			ClusterNamespaceArn: aws.String("dummy-redshift-cluster-arn-3"),
			ClusterIdentifier:   aws.String("redshift-cluster-3"),
		},
	}
	s.dummyDynamoDBTableNames = []string{
		"dynamodb-table-1",
		"dynamodb-table-2",
		"dynamodb-table-3",
		"dynamodb-table-4",
	}
	s.dummyDynamoDBTable = map[string]*types.TableDescription{
		s.dummyDynamoDBTableNames[0]: {
			TableArn:  aws.String("dummy-dynamo-table-arn-1"),
			TableName: aws.String(s.dummyDynamoDBTableNames[0]),
		},
		s.dummyDynamoDBTableNames[1]: {
			TableArn:  aws.String("dummy-dynamo-table-arn-2"),
			TableName: aws.String(s.dummyDynamoDBTableNames[1]),
		},
		s.dummyDynamoDBTableNames[2]: {
			TableArn:  aws.String("dummy-dynamo-table-arn-3"),
			TableName: aws.String(s.dummyDynamoDBTableNames[2]),
		},
		s.dummyDynamoDBTableNames[3]: {
			// We add a duplicate ARN here to test that the scanner will not
			// report the same table twice.
			TableArn:  aws.String("dummy-dynamo-table-arn-3"),
			TableName: aws.String(s.dummyDynamoDBTableNames[3]),
		},
	}
	s.dummyDynamoDBTags = []types.Tag{
		{
			Key:   aws.String("tag1"),
			Value: aws.String("value1"),
		},
		{
			Key:   aws.String("tag2"),
			Value: aws.String("value2"),
		},
		{
			Key:   aws.String("tag3"),
			Value: aws.String("value3"),
		},
	}
	s.dummyS3Buckets = []s3Types.Bucket{
		{
			Name:         aws.String("bucket_1"),
			CreationDate: &time.Time{},
		},
		{
			Name:         aws.String("bucket_2"),
			CreationDate: &time.Time{},
		},
		{
			Name:         aws.String("bucket_3"),
			CreationDate: &time.Time{},
		},
	}
	s.dummyS3Tags = []s3Types.Tag{
		{
			Key:   aws.String("s3tag1"),
			Value: aws.String("s3value1"),
		},
	}
}

func TestAWSScanner(t *testing.T) {
	s := new(AWSScannerTestSuite)
	suite.Run(t, s)
}

func (s *AWSScannerTestSuite) TestScan() {
	awsScanner := AWSScanner{
		scannerConfig: ScannerConfig{
			Regions: []string{
				"us-east-1",
			},
			AssumeRole: &AssumeRoleConfig{
				IAMRoleARN: "arn:aws:iam::123456789012:role/SomeIAMRole",
				ExternalID: "some-external-id-12345",
			},
		},
		awsConfig: aws.Config{},
		awsClientConstructor: func(awsConfig aws.Config) *awsClient {
			return &awsClient{
				config: awsConfig,
				rds: &mockRDSClient{
					DBClusters:  s.dummyRDSClusters,
					DBInstances: s.dummyRDSInstances,
				},
				redshift: &mockRedshiftClient{
					Clusters: s.dummyRedshiftClusters,
				},
				dynamodb: &mockDynamoDBClient{
					TableNames: s.dummyDynamoDBTableNames,
					Table:      s.dummyDynamoDBTable,
					Tags:       s.dummyDynamoDBTags,
				},
				s3: &mockS3Client{
					Buckets: s.dummyS3Buckets,
					Tags:    s.dummyS3Tags,
				},
			}
		},
	}
	ctx := context.Background()
	results, err := awsScanner.Scan(ctx)

	expectedResults := &scan.ScanResults{
		Repositories: map[string]scan.Repository{
			*s.dummyRDSClusters[0].DBClusterArn: {
				Id:         *s.dummyRDSClusters[0].DBClusterArn,
				Name:       *s.dummyRDSClusters[0].DBClusterIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSClusters[0],
			},
			*s.dummyRDSClusters[1].DBClusterArn: {
				Id:         *s.dummyRDSClusters[1].DBClusterArn,
				Name:       *s.dummyRDSClusters[1].DBClusterIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSClusters[1],
			},
			*s.dummyRDSClusters[2].DBClusterArn: {
				Id:         *s.dummyRDSClusters[2].DBClusterArn,
				Name:       *s.dummyRDSClusters[2].DBClusterIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSClusters[2],
			},
			*s.dummyRDSInstances[0].DBInstanceArn: {
				Id:         *s.dummyRDSInstances[0].DBInstanceArn,
				Name:       *s.dummyRDSInstances[0].DBInstanceIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSInstances[0],
			},
			*s.dummyRDSInstances[1].DBInstanceArn: {
				Id:         *s.dummyRDSInstances[1].DBInstanceArn,
				Name:       *s.dummyRDSInstances[1].DBInstanceIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSInstances[1],
			},
			*s.dummyRDSInstances[2].DBInstanceArn: {
				Id:         *s.dummyRDSInstances[2].DBInstanceArn,
				Name:       *s.dummyRDSInstances[2].DBInstanceIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSInstances[2],
			},
			*s.dummyRedshiftClusters[0].ClusterNamespaceArn: {
				Id:         *s.dummyRedshiftClusters[0].ClusterNamespaceArn,
				Name:       *s.dummyRedshiftClusters[0].ClusterIdentifier,
				Type:       scan.RepoTypeRedshift,
				Tags:       []string{},
				Properties: s.dummyRedshiftClusters[0],
			},
			*s.dummyRedshiftClusters[1].ClusterNamespaceArn: {
				Id:         *s.dummyRedshiftClusters[1].ClusterNamespaceArn,
				Name:       *s.dummyRedshiftClusters[1].ClusterIdentifier,
				Type:       scan.RepoTypeRedshift,
				Tags:       []string{},
				Properties: s.dummyRedshiftClusters[1],
			},
			*s.dummyRedshiftClusters[2].ClusterNamespaceArn: {
				Id:         *s.dummyRedshiftClusters[2].ClusterNamespaceArn,
				Name:       *s.dummyRedshiftClusters[2].ClusterIdentifier,
				Type:       scan.RepoTypeRedshift,
				Tags:       []string{},
				Properties: s.dummyRedshiftClusters[2],
			},
			*s.dummyDynamoDBTable[s.dummyDynamoDBTableNames[0]].TableArn: {
				Id:   *s.dummyDynamoDBTable[s.dummyDynamoDBTableNames[0]].TableArn,
				Name: s.dummyDynamoDBTableNames[0],
				Type: scan.RepoTypeDynamoDB,
				Tags: []string{
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDynamoDBTags[0].Key, *s.dummyDynamoDBTags[0].Value,
					),
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDynamoDBTags[1].Key, *s.dummyDynamoDBTags[1].Value,
					),
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDynamoDBTags[2].Key, *s.dummyDynamoDBTags[2].Value,
					),
				},
				Properties: *s.dummyDynamoDBTable[s.dummyDynamoDBTableNames[0]],
			},
			*s.dummyDynamoDBTable[s.dummyDynamoDBTableNames[1]].TableArn: {
				Id:   *s.dummyDynamoDBTable[s.dummyDynamoDBTableNames[1]].TableArn,
				Name: s.dummyDynamoDBTableNames[1],
				Type: scan.RepoTypeDynamoDB,
				Tags: []string{
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDynamoDBTags[0].Key, *s.dummyDynamoDBTags[0].Value,
					),
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDynamoDBTags[1].Key, *s.dummyDynamoDBTags[1].Value,
					),
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDynamoDBTags[2].Key, *s.dummyDynamoDBTags[2].Value,
					),
				},
				Properties: *s.dummyDynamoDBTable[s.dummyDynamoDBTableNames[1]],
			},
			*s.dummyDynamoDBTable[s.dummyDynamoDBTableNames[2]].TableArn: {
				Id:   *s.dummyDynamoDBTable[s.dummyDynamoDBTableNames[2]].TableArn,
				Name: s.dummyDynamoDBTableNames[2],
				Type: scan.RepoTypeDynamoDB,
				Tags: []string{
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDynamoDBTags[0].Key, *s.dummyDynamoDBTags[0].Value,
					),
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDynamoDBTags[1].Key, *s.dummyDynamoDBTags[1].Value,
					),
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDynamoDBTags[2].Key, *s.dummyDynamoDBTags[2].Value,
					),
				},
				Properties: *s.dummyDynamoDBTable[s.dummyDynamoDBTableNames[2]],
			},
		},
	}

	require.Equal(s.T(), expectedResults, results)
	require.NoError(s.T(), err)
}

func (s *AWSScannerTestSuite) TestScan_WithErrors() {
	dummyError := fmt.Errorf("dummy-error")
	awsScanner := AWSScanner{
		scannerConfig: ScannerConfig{
			Regions: []string{
				"us-east-1",
				"us-east-2",
			},
			AssumeRole: &AssumeRoleConfig{
				IAMRoleARN: "arn:aws:iam::123456789012:role/SomeIAMRole",
				ExternalID: "some-external-id-12345",
			},
		},
		awsConfig: aws.Config{},
		awsClientConstructor: func(awsConfig aws.Config) *awsClient {
			return &awsClient{
				config: awsConfig,
				rds: &mockRDSClient{
					Errors: map[string]error{
						"DescribeDBClusters":  dummyError,
						"DescribeDBInstances": dummyError,
					},
				},
				redshift: &mockRedshiftClient{
					Errors: map[string]error{
						"DescribeClusters": dummyError,
					},
				},
				dynamodb: &mockDynamoDBClient{
					Errors: map[string]error{
						"ListTables": dummyError,
					},
				},
				s3: &mockS3Client{
					Errors: map[string]error{
						"ListBuckets":      dummyError,
						"GetBucketTagging": dummyError,
					},
				},
			}
		},
	}

	ctx := context.Background()
	results, err := awsScanner.Scan(ctx)

	expectedResults := &scan.ScanResults{
		Repositories: nil,
	}

	require.ElementsMatch(
		s.T(),
		expectedResults.Repositories,
		results.Repositories,
	)
	require.ErrorIs(s.T(), err, dummyError)
}

type mockRDSClient struct {
	DBClusters  []rdsTypes.DBCluster
	DBInstances []rdsTypes.DBInstance
	Errors      map[string]error
}

func (m *mockRDSClient) DescribeDBClusters(
	_ context.Context,
	params *rds.DescribeDBClustersInput,
	_ ...func(*rds.Options),
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

func (m *mockRDSClient) DescribeDBInstances(
	_ context.Context,
	params *rds.DescribeDBInstancesInput,
	_ ...func(*rds.Options),
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

type mockS3Client struct {
	Buckets []s3Types.Bucket
	Tags    []s3Types.Tag
	Errors  map[string]error
}

func (m *mockS3Client) ListBuckets(
	_ context.Context,
	_ *s3.ListBucketsInput,
	_ ...func(*s3.Options),
) (*s3.ListBucketsOutput, error) {
	if m.Errors["ListBuckets"] != nil {
		return nil, m.Errors["ListBuckets"]
	}

	return &s3.ListBucketsOutput{
		Buckets: m.Buckets,
	}, nil
}

func (m *mockS3Client) GetBucketTagging(
	_ context.Context,
	_ *s3.GetBucketTaggingInput,
	_ ...func(*s3.Options),
) (*s3.GetBucketTaggingOutput, error) {
	if m.Errors["GetBucketTagging"] != nil {
		return nil, m.Errors["GetBucketTagging"]
	}

	return &s3.GetBucketTaggingOutput{
		TagSet: m.Tags,
	}, nil
}

type mockRedshiftClient struct {
	Clusters []redshiftTypes.Cluster
	Errors   map[string]error
}

func (m *mockRedshiftClient) DescribeClusters(
	_ context.Context,
	params *redshift.DescribeClustersInput,
	_ ...func(*redshift.Options),
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

type mockDynamoDBClient struct {
	TableNames []string
	Table      map[string]*dynamodbTypes.TableDescription
	Tags       []dynamodbTypes.Tag
	Errors     map[string]error
}

func (m *mockDynamoDBClient) ListTables(
	_ context.Context,
	params *dynamodb.ListTablesInput,
	_ ...func(*dynamodb.Options),
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

func (m *mockDynamoDBClient) DescribeTable(
	_ context.Context,
	params *dynamodb.DescribeTableInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.DescribeTableOutput, error) {
	if m.Errors["DescribeTable"] != nil {
		return nil, m.Errors["DescribeTable"]
	}
	return &dynamodb.DescribeTableOutput{
		Table: m.Table[*params.TableName],
	}, nil
}

func (m *mockDynamoDBClient) ListTagsOfResource(
	_ context.Context,
	params *dynamodb.ListTagsOfResourceInput,
	_ ...func(*dynamodb.Options),
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
