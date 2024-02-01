package scan

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsType "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
)

type RepoCategory string

const (
	// RepoCategory
	RepoTypeRDS      RepoCategory = "TYPE_RDS"
	RepoTypeRedshift RepoCategory = "TYPE_REDSHIFT"
	RepoTypeDynamoDB RepoCategory = "TYPE_DYNAMODB"
	// Common property keys
	VersionKey          = "version"
	EndpointKey         = "endpoint"
	AddressKey          = "address"
	PortKey             = "port"
	AllocatedSizeKey    = "allocatedSize"
	PublicAccessibleKey = "publicAccessible"
)

type Repository struct {
	Name       string
	CreatedAt  time.Time
	Tags       []string
	Category   RepoCategory
	Properties map[string]any
}

func newRepositoryFromRedshiftCluster(cluster types.Cluster) Repository {
	properties := map[string]any{
		VersionKey:          aws.ToString(cluster.ClusterVersion),
		AllocatedSizeKey:    aws.ToInt64(cluster.TotalStorageCapacityInMegaBytes),
		PublicAccessibleKey: aws.ToBool(cluster.PubliclyAccessible),
		EndpointKey:         map[string]any{},
	}
	if cluster.Endpoint != nil {
		properties[EndpointKey] = map[string]any{
			AddressKey: aws.ToString(cluster.Endpoint.Address),
			PortKey:    aws.ToInt32(cluster.Endpoint.Port),
		}
	}

	tags := make([]string, len(cluster.Tags))
	for _, tag := range cluster.Tags {
		tags = append(tags, fmt.Sprintf(
			"%s:%s",
			aws.ToString(tag.Key),
			aws.ToString(tag.Value),
		))
	}

	repo := Repository{
		Name:       aws.ToString(cluster.ClusterIdentifier),
		CreatedAt:  aws.ToTime(cluster.ClusterCreateTime),
		Category:   RepoTypeRedshift,
		Tags:       tags,
		Properties: properties,
	}

	return repo
}

func newRepositoryFromDynamoDBTable(table dynamoDBTable) Repository {
	return Repository{}
}

func newRepositoryFromRDSCluster(cluster rdsType.DBCluster) Repository {
	return Repository{}
}

func newRepositoryFromRDSInstance(instance rdsType.DBInstance) Repository {
	return Repository{}
}
