package scan

import (
	"context"
	"errors"
	"time"
)

// Scanner is an interface that should be implemented for a specific cloud
// provider (e.g. AWS, GCP, etc.). It defines the Scan method responsible for
// scanning the existing data repositories of the corresponding cloud provider
// environment.
type Scanner interface {
	Scan(ctx context.Context) (*ScanResults, error)
}

// RepoType defines the AWS data repository types supported (e.g. RDS, Redshift,
// DynamoDB, etc).
type RepoType string

const (
	RepoTypeRDS        RepoType = "TYPE_RDS"
	RepoTypeRedshift   RepoType = "TYPE_REDSHIFT"
	RepoTypeDynamoDB   RepoType = "TYPE_DYNAMODB"
	RepoTypeS3         RepoType = "TYPE_S3"
	RepoTypeDocumentDB RepoType = "TYPE_DOCUMENTDB"
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
// data repositories that were scanned. The map key is the repository ID and the
// value is the repository itself.
type ScanResults struct {
	Repositories map[string]Repository
}

// ScanError is an error type that represents a collection of errors that
// occurred during the scanning process.
type ScanError struct {
	Errs []error
}

func (e *ScanError) Error() string {
	if e == nil {
		return ""
	}
	return errors.Join(e.Errs...).Error()
}

func (e *ScanError) Unwrap() []error {
	if e == nil {
		return nil
	}
	return e.Errs
}
