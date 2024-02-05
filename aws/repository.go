package aws

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	redshiftTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/cyralinc/dmap/scan"
)

func newRepositoryFromRDSCluster(
	cluster rdsTypes.DBCluster,
) scan.Repository {
	tags := make([]string, 0, len(cluster.TagList))
	for _, tag := range cluster.TagList {
		tags = append(tags, fmt.Sprintf(
			"%s:%s",
			aws.ToString(tag.Key),
			aws.ToString(tag.Value),
		))
	}

	return scan.Repository{
		Id:         aws.ToString(cluster.DBClusterArn),
		Name:       aws.ToString(cluster.DBClusterIdentifier),
		CreatedAt:  aws.ToTime(cluster.ClusterCreateTime),
		Type:       scan.RepoTypeRDS,
		Tags:       tags,
		Properties: cluster,
	}
}

func newRepositoryFromRDSInstance(
	instance rdsTypes.DBInstance,
) scan.Repository {
	tags := make([]string, 0, len(instance.TagList))
	for _, tag := range instance.TagList {
		tags = append(tags, fmt.Sprintf(
			"%s:%s",
			aws.ToString(tag.Key),
			aws.ToString(tag.Value),
		))
	}

	return scan.Repository{
		Id:         aws.ToString(instance.DBInstanceArn),
		Name:       aws.ToString(instance.DBInstanceIdentifier),
		CreatedAt:  aws.ToTime(instance.InstanceCreateTime),
		Type:       scan.RepoTypeRDS,
		Tags:       tags,
		Properties: instance,
	}
}

func newRepositoryFromRedshiftCluster(
	cluster redshiftTypes.Cluster,
) scan.Repository {
	tags := make([]string, 0, len(cluster.Tags))
	for _, tag := range cluster.Tags {
		tags = append(tags, fmt.Sprintf(
			"%s:%s",
			aws.ToString(tag.Key),
			aws.ToString(tag.Value),
		))
	}

	return scan.Repository{
		Id:         aws.ToString(cluster.ClusterNamespaceArn),
		Name:       aws.ToString(cluster.ClusterIdentifier),
		CreatedAt:  aws.ToTime(cluster.ClusterCreateTime),
		Type:       scan.RepoTypeRedshift,
		Tags:       tags,
		Properties: cluster,
	}
}

func newRepositoryFromDynamoDBTable(
	table dynamoDBTable,
) scan.Repository {
	tags := make([]string, 0, len(table.Tags))
	for _, tag := range table.Tags {
		tags = append(tags, fmt.Sprintf(
			"%s:%s",
			aws.ToString(tag.Key),
			aws.ToString(tag.Value),
		))
	}

	return scan.Repository{
		Id:         aws.ToString(table.Table.TableId),
		Name:       aws.ToString(table.Table.TableName),
		CreatedAt:  aws.ToTime(table.Table.CreationDateTime),
		Type:       scan.RepoTypeDynamoDB,
		Tags:       tags,
		Properties: table.Table,
	}
}
