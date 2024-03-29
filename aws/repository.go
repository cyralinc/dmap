package aws

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	redshiftTypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"

	"github.com/cyralinc/dmap/scan"
)

const (
	docDbEngine = "docdb"
)

func newRepositoryFromRDSCluster(
	cluster rdsTypes.DBCluster,
) scan.Repository {
	tags := make([]string, 0, len(cluster.TagList))
	for _, tag := range cluster.TagList {
		tags = append(tags, formatTag(tag.Key, tag.Value))
	}
	repoType := scan.RepoTypeRDS
	if cluster.Engine != nil && *cluster.Engine == docDbEngine {
		repoType = scan.RepoTypeDocumentDB
	}
	return scan.Repository{
		Id:         aws.ToString(cluster.DBClusterArn),
		Name:       aws.ToString(cluster.DBClusterIdentifier),
		CreatedAt:  aws.ToTime(cluster.ClusterCreateTime),
		Type:       repoType,
		Tags:       tags,
		Properties: cluster,
	}
}

func newRepositoryFromRDSInstance(
	instance rdsTypes.DBInstance,
) scan.Repository {
	tags := make([]string, 0, len(instance.TagList))
	for _, tag := range instance.TagList {
		tags = append(tags, formatTag(tag.Key, tag.Value))
	}
	repoType := scan.RepoTypeRDS
	if instance.Engine != nil && *instance.Engine == docDbEngine {
		repoType = scan.RepoTypeDocumentDB
	}
	return scan.Repository{
		Id:         aws.ToString(instance.DBInstanceArn),
		Name:       aws.ToString(instance.DBInstanceIdentifier),
		CreatedAt:  aws.ToTime(instance.InstanceCreateTime),
		Type:       repoType,
		Tags:       tags,
		Properties: instance,
	}
}

func newRepositoryFromRedshiftCluster(
	cluster redshiftTypes.Cluster,
) scan.Repository {
	tags := make([]string, 0, len(cluster.Tags))
	for _, tag := range cluster.Tags {
		tags = append(tags, formatTag(tag.Key, tag.Value))
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
		tags = append(tags, formatTag(tag.Key, tag.Value))
	}

	return scan.Repository{
		Id:         aws.ToString(table.Table.TableArn),
		Name:       aws.ToString(table.Table.TableName),
		CreatedAt:  aws.ToTime(table.Table.CreationDateTime),
		Type:       scan.RepoTypeDynamoDB,
		Tags:       tags,
		Properties: table.Table,
	}
}

func formatTag(key, value *string) string {
	return fmt.Sprintf(
		"%s:%s",
		aws.ToString(key),
		aws.ToString(value),
	)
}

func bucketNameToARN(name string) string {
	return fmt.Sprintf("arn:aws:s3:::%s", name)
}

func newRepositoryFromS3Bucket(
	bucket S3Bucket,
) scan.Repository {
	return scan.Repository{
		Id:        bucketNameToARN(*bucket.bucket.Name),
		Name:      *bucket.bucket.Name,
		Type:      scan.RepoTypeS3,
		CreatedAt: *bucket.bucket.CreationDate,
		Tags:      bucket.tags,
	}
}
