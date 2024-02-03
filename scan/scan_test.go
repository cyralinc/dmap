package scan

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	redshiftTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"

	"github.com/cyralinc/dmap/config"
	"github.com/cyralinc/dmap/model"
	"github.com/cyralinc/dmap/testutil/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ScanManagerTestSuite struct {
	suite.Suite
	dummyRepos []model.Repository
}

func (s *ScanManagerTestSuite) SetupSuite() {
	s.dummyRepos = []model.Repository{
		{
			Id:        "1",
			Name:      "some-rds-instance",
			Type:      model.RepoTypeRDS,
			CreatedAt: time.Now(),
			Tags:      []string{"tag1", "tag2"},
			Properties: rdsTypes.DBInstance{
				DBInstanceIdentifier: aws.String("rds-instance-1"),
			},
		},
		{
			Id:        "2",
			Name:      "some-redshift-cluster",
			Type:      model.RepoTypeRedshift,
			CreatedAt: time.Now(),
			Tags:      []string{"tag1"},
			Properties: redshiftTypes.Cluster{
				ClusterIdentifier: aws.String("redshift-cluster-1"),
			},
		},
		{
			Id:        "3",
			Name:      "some-rds-cluster",
			Type:      model.RepoTypeRDS,
			CreatedAt: time.Now(),
			Tags:      []string{},
			Properties: rdsTypes.DBCluster{
				DBClusterIdentifier: aws.String("rds-cluster-1"),
			},
		},
		{
			Id:        "4",
			Name:      "some-dynamodb-table",
			Type:      model.RepoTypeDynamoDB,
			CreatedAt: time.Now(),
			Tags:      nil,
			Properties: ddbTypes.TableDescription{
				TableName: aws.String("dynamodb-table-1"),
			},
		},
	}
}

func TestScanManager(t *testing.T) {
	s := new(ScanManagerTestSuite)
	suite.Run(t, s)
}

func (s *ScanManagerTestSuite) TestScanRepositories() {
	err1 := fmt.Errorf("Error during scanner 1")
	err2 := fmt.Errorf("Error during scanner 2")
	manager := ScanManager{
		config: config.Config{
			AWS: &config.AWSConfig{
				Regions: []string{
					"us-east-1",
					"us-east-2",
					"us-west-1",
					"us-west-2",
				},
				AssumeRole: &config.AWSAssumeRoleConfig{
					IAMRoleARN: "arn:aws:iam::123456789012:role/SomeIAMRole",
					ExternalID: "some-external-id-12345",
				},
			},
		},
		scanners: []Scanner{
			&mock.MockScanner{
				Repositories: []model.Repository{
					s.dummyRepos[0],
					s.dummyRepos[1],
				},
				Err: err1,
			},
			&mock.MockScanner{
				Repositories: []model.Repository{
					s.dummyRepos[2],
					s.dummyRepos[3],
				},
				Err: err2,
			},
		},
	}
	ctx := context.Background()
	scanResults, err := manager.ScanRepositories(ctx)

	expectedscanResults := &ScanResults{
		Repositories: s.dummyRepos,
	}
	expectedError := errors.Join(err1, err2)

	require.Equal(s.T(), expectedscanResults, scanResults)
	require.EqualError(s.T(), err, expectedError.Error())
}
