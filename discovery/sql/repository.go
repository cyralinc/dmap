package sql

import (
	"context"
)

// Repository represents a Dmap data SQL repository, and provides functionality
// to introspect its corresponding schema.
type Repository interface {
	// ListDatabases returns a list of the names of all databases on the server.
	ListDatabases(ctx context.Context) ([]string, error)
	// Introspect will read and analyze the basic properties of the repository
	// and return as a Metadata instance. This includes all the repository's
	// databases, schemas, tables, columns, and attributes.
	Introspect(ctx context.Context) (*Metadata, error)
	// SampleTable samples the table referenced by the TableMetadata meta
	// parameter and returns the sample as a slice of Sample. The parameters for
	// the sample, such as sample size, are passed via the params parameter (see
	// SampleParameters for more details). The returned sample result set
	// contains one Sample for each table row sampled. The length of the results
	// will be less than or equal to the sample size. If there are fewer results
	// than the specified sample size, it is because the table in question had a
	// row count less than the sample size. Prefer small sample sizes to limit
	// impact on the database.
	SampleTable(ctx context.Context, meta *TableMetadata, params SampleParameters) (Sample, error)
	// Ping is meant to be used as a general purpose connectivity test. It
	// should be invoked e.g. in the dry-run mode.
	Ping(ctx context.Context) error
	// Close is meant to be used as a general purpose cleanup. It should be
	// invoked when the Repository is no longer used.
	Close() error
}
