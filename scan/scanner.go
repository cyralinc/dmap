package scan

import (
	"context"
	"errors"
	"time"

	"github.com/cyralinc/dmap/classification"
)

// Scanner provides an API to scan cloud environments. It should be
// implemented for a specific cloud provider (e.g. AWS, GCP, etc.). It defines
// the Scan method responsible for discovering the existing data repositories in
// a cloud environment.
type Scanner interface {
	Scan(ctx context.Context) (*ScanResults, error)
}

// RepoScanner is a scanner that scans a data repository for sensitive data.
type RepoScanner interface {
	Scan(ctx context.Context) (*RepoScanResults, error)
}

// RepoScanResults is the result of a repository scan.
type RepoScanResults struct {
	Labels          []classification.Label          `json:"labels"`
	Classifications []classification.Classification `json:"classifications"`
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

// Error returns a string representation of the error.
func (e *ScanError) Error() string {
	if e == nil {
		return ""
	}
	return errors.Join(e.Errs...).Error()
}

// Unwrap returns the list of errors that occurred during the scanning process.
func (e *ScanError) Unwrap() []error {
	if e == nil {
		return nil
	}
	return e.Errs
}
