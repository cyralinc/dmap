package aws

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/scan"
)

func Test_newRepositoryFromRDSCluster(t *testing.T) {
	tests := []struct {
		name    string
		cluster types.DBCluster
		want    scan.Repository
	}{
		{
			name: "postgres cluster",
			cluster: types.DBCluster{
				DBClusterArn:        ptr("arn:aws:rds:us-west-2:123456789012:cluster:my-cluster"),
				DBClusterIdentifier: ptr("my-cluster"),
				ClusterCreateTime:   ptr(time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)),
				Engine:              ptr("postgres"),
				TagList:             []types.Tag{{Key: ptr("key"), Value: ptr("value")}},
			},
			want: scan.Repository{
				Id:        "arn:aws:rds:us-west-2:123456789012:cluster:my-cluster",
				Name:      "my-cluster",
				Type:      scan.RepoTypeRDS,
				CreatedAt: time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC),
				Tags:      []string{"key:value"},
			},
		},
		{
			name: "nil engine cluster",
			cluster: types.DBCluster{
				DBClusterArn:        ptr("arn:aws:rds:us-west-2:123456789012:cluster:my-cluster"),
				DBClusterIdentifier: ptr("my-cluster"),
				ClusterCreateTime:   ptr(time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)),
				Engine:              nil,
				TagList:             []types.Tag{{Key: ptr("key"), Value: ptr("value")}},
			},
			want: scan.Repository{
				Id:        "arn:aws:rds:us-west-2:123456789012:cluster:my-cluster",
				Name:      "my-cluster",
				Type:      scan.RepoTypeRDS,
				CreatedAt: time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC),
				Tags:      []string{"key:value"},
			},
		},
		{
			name: "docdb cluster",
			cluster: types.DBCluster{
				DBClusterArn:        ptr("arn:aws:rds:us-west-2:123456789012:cluster:my-cluster"),
				DBClusterIdentifier: ptr("my-cluster"),
				ClusterCreateTime:   ptr(time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)),
				Engine:              ptr("docdb"),
			},
			want: scan.Repository{
				Id:        "arn:aws:rds:us-west-2:123456789012:cluster:my-cluster",
				Name:      "my-cluster",
				Type:      scan.RepoTypeDocumentDB,
				CreatedAt: time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC),
				Tags:      []string{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got := newRepositoryFromRDSCluster(tt.cluster)
				require.Equal(t, tt.want.Id, got.Id)
				require.Equal(t, tt.want.Name, got.Name)
				require.Equal(t, tt.want.CreatedAt, got.CreatedAt)
				require.Equal(t, tt.want.Type, got.Type)
				require.Equal(t, tt.want.Tags, got.Tags)
				require.IsType(t, types.DBCluster{}, got.Properties)
			},
		)
	}
}

func Test_newRepositoryFromRDSInstance(t *testing.T) {
	tests := []struct {
		name     string
		instance types.DBInstance
		want     scan.Repository
	}{
		{
			name: "postgres instance",
			instance: types.DBInstance{
				DBInstanceArn:        ptr("arn:aws:rds:us-west-2:123456789012:instance:my-instance"),
				DBInstanceIdentifier: ptr("my-instance"),
				InstanceCreateTime:   ptr(time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)),
				Engine:               ptr("postgres"),
				TagList:              []types.Tag{{Key: ptr("key"), Value: ptr("value")}},
			},
			want: scan.Repository{
				Id:        "arn:aws:rds:us-west-2:123456789012:instance:my-instance",
				Name:      "my-instance",
				Type:      scan.RepoTypeRDS,
				CreatedAt: time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC),
				Tags:      []string{"key:value"},
			},
		},
		{
			name: "nil engine instance",
			instance: types.DBInstance{
				DBInstanceArn:        ptr("arn:aws:rds:us-west-2:123456789012:instance:my-instance"),
				DBInstanceIdentifier: ptr("my-instance"),
				InstanceCreateTime:   ptr(time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)),
				Engine:               nil,
				TagList:              []types.Tag{{Key: ptr("key"), Value: ptr("value")}},
			},
			want: scan.Repository{
				Id:        "arn:aws:rds:us-west-2:123456789012:instance:my-instance",
				Name:      "my-instance",
				Type:      scan.RepoTypeRDS,
				CreatedAt: time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC),
				Tags:      []string{"key:value"},
			},
		},
		{
			name: "docdb instance",
			instance: types.DBInstance{
				DBInstanceArn:        ptr("arn:aws:rds:us-west-2:123456789012:instance:my-instance"),
				DBInstanceIdentifier: ptr("my-instance"),
				InstanceCreateTime:   ptr(time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)),
				Engine:               ptr("docdb"),
			},
			want: scan.Repository{
				Id:        "arn:aws:rds:us-west-2:123456789012:instance:my-instance",
				Name:      "my-instance",
				Type:      scan.RepoTypeDocumentDB,
				CreatedAt: time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC),
				Tags:      []string{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got := newRepositoryFromRDSInstance(tt.instance)
				require.Equal(t, tt.want.Id, got.Id)
				require.Equal(t, tt.want.Name, got.Name)
				require.Equal(t, tt.want.CreatedAt, got.CreatedAt)
				require.Equal(t, tt.want.Type, got.Type)
				require.Equal(t, tt.want.Tags, got.Tags)
				require.IsType(t, types.DBInstance{}, got.Properties)
			},
		)
	}
}

func ptr[T any](v T) *T {
	return &v
}
