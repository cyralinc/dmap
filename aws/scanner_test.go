package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbTypes "github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	redshiftTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cyralinc/dmap/scan"
	"github.com/cyralinc/dmap/testutil/mock"
)

type AWSScannerTestSuite struct {
	suite.Suite

	dummyRDSClusters  []rdsTypes.DBCluster
	dummyRDSInstances []rdsTypes.DBInstance

	dummyRedshiftClusters []redshiftTypes.Cluster

	dummyDynamoDBTableNames []string
	dummyDynamoDBTable      map[string]*types.TableDescription
	dummyDynamoDBTags       []types.Tag

	dummyDocumentDBClusters []docdbTypes.DBCluster
	dummyDocumentDBTags     []docdbTypes.Tag

	dummyS3Buckets []s3Types.Bucket
	dummyS3Tags    []s3Types.Tag
}

func (s *AWSScannerTestSuite) SetupSuite() {
	s.dummyRDSClusters = []rdsTypes.DBCluster{
		{
			DBClusterArn:        aws.String("dummy-arn-1"),
			DBClusterIdentifier: aws.String("rds-cluster-1"),
		},
		{
			DBClusterArn:        aws.String("dummy-arn-2"),
			DBClusterIdentifier: aws.String("rds-cluster-2"),
		},
		{
			DBClusterArn:        aws.String("dummy-arn-3"),
			DBClusterIdentifier: aws.String("rds-cluster-3"),
		},
	}
	s.dummyRDSInstances = []rdsTypes.DBInstance{
		{
			DBInstanceArn:        aws.String("dummy-arn-1"),
			DBInstanceIdentifier: aws.String("rds-instance-1"),
		},
		{
			DBInstanceArn:        aws.String("dummy-arn-2"),
			DBInstanceIdentifier: aws.String("rds-instance-2"),
		},
		{
			DBInstanceArn:        aws.String("dummy-arn-3"),
			DBInstanceIdentifier: aws.String("rds-instance-3"),
		},
	}
	s.dummyRedshiftClusters = []redshiftTypes.Cluster{
		{
			ClusterNamespaceArn: aws.String("dummy-arn-1"),
			ClusterIdentifier:   aws.String("redshift-cluster-1"),
		},
		{
			ClusterNamespaceArn: aws.String("dummy-arn-2"),
			ClusterIdentifier:   aws.String("redshift-cluster-2"),
		},
		{
			ClusterNamespaceArn: aws.String("dummy-arn-3"),
			ClusterIdentifier:   aws.String("redshift-cluster-3"),
		},
	}
	s.dummyDynamoDBTableNames = []string{
		"dynamodb-table-1",
		"dynamodb-table-2",
		"dynamodb-table-3",
	}
	s.dummyDynamoDBTable = map[string]*types.TableDescription{
		s.dummyDynamoDBTableNames[0]: {
			TableArn:  aws.String("dummy-arn-1"),
			TableName: aws.String(s.dummyDynamoDBTableNames[0]),
		},
		s.dummyDynamoDBTableNames[1]: {
			TableArn:  aws.String("dummy-arn-2"),
			TableName: aws.String(s.dummyDynamoDBTableNames[1]),
		},
		s.dummyDynamoDBTableNames[2]: {
			TableArn:  aws.String("dummy-arn-3"),
			TableName: aws.String(s.dummyDynamoDBTableNames[2]),
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

	s.dummyDocumentDBClusters = []docdbTypes.DBCluster{
		{
			DBClusterArn:        aws.String("documentdb-arn-1"),
			DBClusterIdentifier: aws.String("documentdb-cluster-1"),
			ClusterCreateTime:   &time.Time{},
		},
		{
			DBClusterArn:        aws.String("documentdb-arn-2"),
			DBClusterIdentifier: aws.String("documentdb-cluster-2"),
			ClusterCreateTime:   &time.Time{},
		},
		{
			DBClusterArn:        aws.String("documentdb-arn-3"),
			DBClusterIdentifier: aws.String("documentdb-cluster-3"),
			ClusterCreateTime:   &time.Time{},
		},
	}
	s.dummyDocumentDBTags = []docdbTypes.Tag{
		{
			Key:   aws.String("docdbTag1"),
			Value: aws.String("docdbValue1"),
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
				rds: &mock.MockRDSClient{
					DBClusters:  s.dummyRDSClusters,
					DBInstances: s.dummyRDSInstances,
				},
				redshift: &mock.MockRedshiftClient{
					Clusters: s.dummyRedshiftClusters,
				},
				dynamodb: &mock.MockDynamoDBClient{
					TableNames: s.dummyDynamoDBTableNames,
					Table:      s.dummyDynamoDBTable,
					Tags:       s.dummyDynamoDBTags,
				},
				docdb: &mock.MockDocumentDBClient{
					Clusters: s.dummyDocumentDBClusters,
					Tags:     s.dummyDocumentDBTags,
				},
				s3: &mock.MockS3Client{
					Buckets: s.dummyS3Buckets,
					Tags:    s.dummyS3Tags,
				},
			}
		},
	}
	ctx := context.Background()
	results, err := awsScanner.Scan(ctx)

	expectedResults := &scan.ScanResults{
		Repositories: []scan.Repository{
			{
				Id:         *s.dummyRDSClusters[0].DBClusterArn,
				Name:       *s.dummyRDSClusters[0].DBClusterIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSClusters[0],
			},
			{
				Id:         *s.dummyRDSClusters[1].DBClusterArn,
				Name:       *s.dummyRDSClusters[1].DBClusterIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSClusters[1],
			},
			{
				Id:         *s.dummyRDSClusters[2].DBClusterArn,
				Name:       *s.dummyRDSClusters[2].DBClusterIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSClusters[2],
			},
			{
				Id:         *s.dummyRDSInstances[0].DBInstanceArn,
				Name:       *s.dummyRDSInstances[0].DBInstanceIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSInstances[0],
			},
			{
				Id:         *s.dummyRDSInstances[1].DBInstanceArn,
				Name:       *s.dummyRDSInstances[1].DBInstanceIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSInstances[1],
			},
			{
				Id:         *s.dummyRDSInstances[2].DBInstanceArn,
				Name:       *s.dummyRDSInstances[2].DBInstanceIdentifier,
				Type:       scan.RepoTypeRDS,
				Tags:       []string{},
				Properties: s.dummyRDSInstances[2],
			},
			{
				Id:         *s.dummyRedshiftClusters[0].ClusterNamespaceArn,
				Name:       *s.dummyRedshiftClusters[0].ClusterIdentifier,
				Type:       scan.RepoTypeRedshift,
				Tags:       []string{},
				Properties: s.dummyRedshiftClusters[0],
			},
			{
				Id:         *s.dummyRedshiftClusters[1].ClusterNamespaceArn,
				Name:       *s.dummyRedshiftClusters[1].ClusterIdentifier,
				Type:       scan.RepoTypeRedshift,
				Tags:       []string{},
				Properties: s.dummyRedshiftClusters[1],
			},
			{
				Id:         *s.dummyRedshiftClusters[2].ClusterNamespaceArn,
				Name:       *s.dummyRedshiftClusters[2].ClusterIdentifier,
				Type:       scan.RepoTypeRedshift,
				Tags:       []string{},
				Properties: s.dummyRedshiftClusters[2],
			},
			{
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
			{
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
			{
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
			{
				Id:        *s.dummyDocumentDBClusters[0].DBClusterArn,
				Name:      *s.dummyDocumentDBClusters[0].DBClusterIdentifier,
				Type:      scan.RepoTypeDocumentDB,
				CreatedAt: *s.dummyDocumentDBClusters[0].ClusterCreateTime,
				Tags: []string{
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDocumentDBTags[0].Key, *s.dummyDocumentDBTags[0].Value,
					),
				},
				Properties: s.dummyDocumentDBClusters[0],
			},
			{
				Id:        *s.dummyDocumentDBClusters[1].DBClusterArn,
				Name:      *s.dummyDocumentDBClusters[1].DBClusterIdentifier,
				Type:      scan.RepoTypeDocumentDB,
				CreatedAt: *s.dummyDocumentDBClusters[1].ClusterCreateTime,
				Tags: []string{
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDocumentDBTags[0].Key, *s.dummyDocumentDBTags[0].Value,
					),
				},
				Properties: s.dummyDocumentDBClusters[1],
			},
			{
				Id:        *s.dummyDocumentDBClusters[2].DBClusterArn,
				Name:      *s.dummyDocumentDBClusters[2].DBClusterIdentifier,
				Type:      scan.RepoTypeDocumentDB,
				CreatedAt: *s.dummyDocumentDBClusters[2].ClusterCreateTime,
				Tags: []string{
					fmt.Sprintf(
						"%s:%s",
						*s.dummyDocumentDBTags[0].Key, *s.dummyDocumentDBTags[0].Value,
					),
				},
				Properties: s.dummyDocumentDBClusters[2],
			},
			{
				Id:        "arn:aws:s3:::bucket_1",
				Name:      "bucket_1",
				Type:      scan.RepoTypeS3,
				CreatedAt: *s.dummyS3Buckets[0].CreationDate,
				Tags: []string{
					fmt.Sprintf(
						"%s:%s",
						*s.dummyS3Tags[0].Key,
						*s.dummyS3Tags[0].Value,
					),
				},
			},
			{
				Id:        "arn:aws:s3:::bucket_2",
				Name:      "bucket_2",
				Type:      scan.RepoTypeS3,
				CreatedAt: *s.dummyS3Buckets[1].CreationDate,
				Tags: []string{
					fmt.Sprintf(
						"%s:%s",
						*s.dummyS3Tags[0].Key,
						*s.dummyS3Tags[0].Value,
					),
				},
			},
			{
				Id:        "arn:aws:s3:::bucket_3",
				Name:      "bucket_3",
				Type:      scan.RepoTypeS3,
				CreatedAt: *s.dummyS3Buckets[2].CreationDate,
				Tags: []string{
					fmt.Sprintf(
						"%s:%s",
						*s.dummyS3Tags[0].Key,
						*s.dummyS3Tags[0].Value,
					),
				},
			},
		},
	}

	//l := len(expectedResults.Repositories)
	require.ElementsMatch(
		s.T(),
		expectedResults.Repositories,
		results.Repositories,
	)
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
				rds: &mock.MockRDSClient{
					Errors: map[string]error{
						"DescribeDBClusters":  dummyError,
						"DescribeDBInstances": dummyError,
					},
				},
				redshift: &mock.MockRedshiftClient{
					Errors: map[string]error{
						"DescribeClusters": dummyError,
					},
				},
				dynamodb: &mock.MockDynamoDBClient{
					Errors: map[string]error{
						"ListTables": dummyError,
					},
				},
				docdb: &mock.MockDocumentDBClient{
					Errors: map[string]error{
						"DescribeDBClusters":  dummyError,
						"DescribeDBInstances": dummyError,
						"ListTagsForResource": dummyError,
					},
				},
				s3: &mock.MockS3Client{
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
		Repositories: []scan.Repository{},
	}

	require.ElementsMatch(
		s.T(),
		expectedResults.Repositories,
		results.Repositories,
	)
	require.ErrorIs(s.T(), err, dummyError)
}
