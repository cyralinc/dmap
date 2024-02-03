package aws

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	// ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	redshiftTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cyralinc/dmap/config"
	"github.com/cyralinc/dmap/model"
	"github.com/cyralinc/dmap/testutil/mock"
)

type AWSScannerTestSuite struct {
	suite.Suite
	dummyRDSClusters        []rdsTypes.DBCluster
	dummyRDSInstances       []rdsTypes.DBInstance
	dummyRedshiftClusters   []redshiftTypes.Cluster
	dummyDynamoDBTableNames []string
	dummyDynamoDBTable      map[string]*types.TableDescription
	dummyDynamoDBTags       []types.Tag
}

func (s *AWSScannerTestSuite) SetupSuite() {
	s.dummyRDSClusters = []rdsTypes.DBCluster{
		{
			DBClusterIdentifier: aws.String("rds-cluster-1"),
		},
		{
			DBClusterIdentifier: aws.String("rds-cluster-2"),
		},
		{
			DBClusterIdentifier: aws.String("rds-cluster-3"),
		},
	}
	s.dummyRDSInstances = []rdsTypes.DBInstance{
		{
			DBInstanceIdentifier: aws.String("rds-instance-1"),
		},
		{
			DBInstanceIdentifier: aws.String("rds-instance-2"),
		},
		{
			DBInstanceIdentifier: aws.String("rds-instance-3"),
		},
	}
	s.dummyRedshiftClusters = []redshiftTypes.Cluster{
		{
			ClusterIdentifier: aws.String("redshift-cluster-1"),
		},
		{
			ClusterIdentifier: aws.String("redshift-cluster-2"),
		},
		{
			ClusterIdentifier: aws.String("redshift-cluster-3"),
		},
	}
	s.dummyDynamoDBTableNames = []string{
		"dynamodb-table-1",
		"dynamodb-table-2",
		"dynamodb-table-3",
	}
	s.dummyDynamoDBTable = map[string]*types.TableDescription{
		s.dummyDynamoDBTableNames[0]: &types.TableDescription{
			TableName: aws.String(s.dummyDynamoDBTableNames[0]),
		},
		s.dummyDynamoDBTableNames[1]: &types.TableDescription{
			TableName: aws.String(s.dummyDynamoDBTableNames[1]),
		},
		s.dummyDynamoDBTableNames[2]: &types.TableDescription{
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
}

func TestAWSScanner(t *testing.T) {
	s := new(AWSScannerTestSuite)
	suite.Run(t, s)
}

func (s *AWSScannerTestSuite) TestScan() {
	region := "us-east-1"
	awsScanner := AWSScanner{
		scanConfig: config.AWSConfig{
			Regions: []string{region},
			AssumeRole: &config.AWSAssumeRoleConfig{
				IAMRoleARN: "arn:aws:iam::123456789012:role/SomeIAMRole",
				ExternalID: "some-external-id-12345",
			},
		},
		awsConfig: aws.Config{
			Region: region,
		},
		rdsClient: &mock.MockRDSClient{
			DBClusters:  s.dummyRDSClusters,
			DBInstances: s.dummyRDSInstances,
		},
		redshiftClient: &mock.MockRedshiftClient{
			Clusters: s.dummyRedshiftClusters,
		},
		dynamodbClient: &mock.MockDynamoDBClient{
			TableNames: s.dummyDynamoDBTableNames,
			Table:      s.dummyDynamoDBTable,
			Tags:       s.dummyDynamoDBTags,
		},
	}
	ctx := context.Background()
	repositories, err := awsScanner.Scan(ctx)

	expectedRepositories := []model.Repository{
		{
			Name:       *s.dummyRDSClusters[0].DBClusterIdentifier,
			Type:       model.RepoTypeRDS,
			Tags:       []string{},
			Properties: s.dummyRDSClusters[0],
		},
		{
			Name:       *s.dummyRDSClusters[1].DBClusterIdentifier,
			Type:       model.RepoTypeRDS,
			Tags:       []string{},
			Properties: s.dummyRDSClusters[1],
		},
		{
			Name:       *s.dummyRDSClusters[2].DBClusterIdentifier,
			Type:       model.RepoTypeRDS,
			Tags:       []string{},
			Properties: s.dummyRDSClusters[2],
		},
		{
			Name:       *s.dummyRDSInstances[0].DBInstanceIdentifier,
			Type:       model.RepoTypeRDS,
			Tags:       []string{},
			Properties: s.dummyRDSInstances[0],
		},
		{
			Name:       *s.dummyRDSInstances[1].DBInstanceIdentifier,
			Type:       model.RepoTypeRDS,
			Tags:       []string{},
			Properties: s.dummyRDSInstances[1],
		},
		{
			Name:       *s.dummyRDSInstances[2].DBInstanceIdentifier,
			Type:       model.RepoTypeRDS,
			Tags:       []string{},
			Properties: s.dummyRDSInstances[2],
		},
		{
			Name:       *s.dummyRedshiftClusters[0].ClusterIdentifier,
			Type:       model.RepoTypeRedshift,
			Tags:       []string{},
			Properties: s.dummyRedshiftClusters[0],
		},
		{
			Name:       *s.dummyRedshiftClusters[1].ClusterIdentifier,
			Type:       model.RepoTypeRedshift,
			Tags:       []string{},
			Properties: s.dummyRedshiftClusters[1],
		},
		{
			Name:       *s.dummyRedshiftClusters[2].ClusterIdentifier,
			Type:       model.RepoTypeRedshift,
			Tags:       []string{},
			Properties: s.dummyRedshiftClusters[2],
		},
		{
			Name: s.dummyDynamoDBTableNames[0],
			Type: model.RepoTypeDynamoDB,
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
			Name: s.dummyDynamoDBTableNames[1],
			Type: model.RepoTypeDynamoDB,
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
			Name: s.dummyDynamoDBTableNames[2],
			Type: model.RepoTypeDynamoDB,
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
	}

	require.ElementsMatch(s.T(), expectedRepositories, repositories)
	require.NoError(s.T(), err)
}

func (s *AWSScannerTestSuite) TestScan_WithErrors() {
	region := "us-east-1"
	dummyError := fmt.Errorf("dummy-error")
	awsScanner := AWSScanner{
		scanConfig: config.AWSConfig{
			Regions: []string{region},
			AssumeRole: &config.AWSAssumeRoleConfig{
				IAMRoleARN: "arn:aws:iam::123456789012:role/SomeIAMRole",
				ExternalID: "some-external-id-12345",
			},
		},
		awsConfig: aws.Config{
			Region: region,
		},
		rdsClient: &mock.MockRDSClient{
			Errors: map[string]error{
				"DescribeDBClusters":  dummyError,
				"DescribeDBInstances": dummyError,
			},
		},
		redshiftClient: &mock.MockRedshiftClient{
			Errors: map[string]error{
				"DescribeClusters": dummyError,
			},
		},
		dynamodbClient: &mock.MockDynamoDBClient{
			Errors: map[string]error{
				"ListTables": dummyError,
			},
		},
	}
	ctx := context.Background()
	repositories, err := awsScanner.Scan(ctx)

	expectedRepositories := []model.Repository{}
	expectedErrorSubstring := dummyError.Error()

	require.ElementsMatch(s.T(), expectedRepositories, repositories)
	require.ErrorContains(s.T(), err, expectedErrorSubstring)
}
