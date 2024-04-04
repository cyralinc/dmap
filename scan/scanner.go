package scan

import (
	"context"
	"errors"
	"time"

	"github.com/cyralinc/dmap/classification"
)

// Scanner provides an API to scan cloud environments. It should be
// implemented for a specific cloud provider (e.g. AWS, GCP, etc.). It defines
// the Scan method responsible for scanning the existing data repositories of
// the corresponding cloud provider environment.
type Scanner interface {
	Scan(ctx context.Context) (*ScanResults, error)
}

// RepoScanner is a scanner that scans a data repository for sensitive data.
type RepoScanner interface {
	Scan(ctx context.Context) (*RepoScanResults, error)
}

// RepoScanResults is the result of a repository scan.
type RepoScanResults struct {
	Labels          []classification.Label `json:"labels"`
	Classifications []Classification       `json:"classifications"`
}

// Classification represents the classification of a data repository attribute.
type Classification struct {
	// AttributePath is the full path of the data repository attribute
	// (e.g. the column). Each element corresponds to a component, in increasing
	// order of granularity (e.g. [database, schema, table, column]).
	AttributePath []string `json:"attributePath"`
	// Labels is the set of labels that the attribute was classified as.
	Labels classification.LabelSet `json:"labels"`
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
