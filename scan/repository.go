package scan

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsType "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
)

type RepoType string

const (
	// Repo types
	RepoTypeRDS      RepoType = "TYPE_RDS"
	RepoTypeRedshift RepoType = "TYPE_REDSHIFT"
	RepoTypeDynamoDB RepoType = "TYPE_DYNAMODB"
)

type Repository struct {
	Name       string
	CreatedAt  time.Time
	Tags       []string
	Type       RepoType
	Properties map[string]any
}

func newRepositoryFromRedshiftCluster(cluster types.Cluster) Repository {
	tags := make([]string, 0, len(cluster.Tags))
	for _, tag := range cluster.Tags {
		tags = append(tags, fmt.Sprintf(
			"%s:%s",
			aws.ToString(tag.Key),
			aws.ToString(tag.Value),
		))
	}

	bytes, err := json.Marshal(cluster)
	if err != nil {
		log.Printf("%v", err)
	}
	var properties map[string]any
	json.Unmarshal(bytes, &properties)
	// TODO: delete map entries corresponding to high-level fields.

	repo := Repository{
		Name:       aws.ToString(cluster.ClusterIdentifier),
		CreatedAt:  aws.ToTime(cluster.ClusterCreateTime),
		Type:       RepoTypeRedshift,
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
