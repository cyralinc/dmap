package scan

import (
	"context"
	"time"
)

// Scanner is an interface that should be implemented for a specific cloud
// provider (e.g. AWS, GCP, etc). It defines the Scan method responsible for
// scanning the existing data repositories of the corresponding cloud provider
// environment.
type Scanner interface {
	Scan(ctx context.Context) (*ScanResults, error)
}

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
