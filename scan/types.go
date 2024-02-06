package scan

import (
	"time"
)

// RepoType defines the AWS data repository types supported (e.g. RDS, Redshift,
// DynamoDB, etc).
type RepoType string

const (
	// Repo types
	RepoTypeRDS      RepoType = "REPO_TYPE_RDS"
	RepoTypeRedshift RepoType = "REPO_TYPE_REDSHIFT"
	RepoTypeDynamoDB RepoType = "REPO_TYPE_DYNAMODB"
)

// Repository represents a scanned data repository.
type Repository struct {
	Id         string
	Name       string
	Type       RepoType
	CreatedAt  time.Time
	Tags       []string
	Properties any
}

// ScanResults represents the results of a repository scan, including all the
// data repositories that were scanned.
type ScanResults struct {
	Repositories []Repository
}
