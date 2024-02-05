package scan

import (
	"time"
)

type RepoType string

const (
	// Repo types
	RepoTypeRDS      RepoType = "REPO_TYPE_RDS"
	RepoTypeRedshift RepoType = "REPO_TYPE_REDSHIFT"
	RepoTypeDynamoDB RepoType = "REPO_TYPE_DYNAMODB"
)

type Repository struct {
	Id         string
	Name       string
	Type       RepoType
	CreatedAt  time.Time
	Tags       []string
	Properties any
}

type ScanResults struct {
	Repositories []Repository
}
