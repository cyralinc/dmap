package sql

import (
	"context"

	"github.com/gobwas/glob"
)

// Repository represents a Dmap data SQL repository, and provides functionality
// to introspect its corresponding schema.
type Repository interface {
	// ListDatabases returns a list of the names of all databases on the server.
	ListDatabases(ctx context.Context) ([]string, error)
	// Introspect will read and analyze the basic properties of the repository
	// and return as a Metadata instance. This includes all the repository's
	// databases, schemas, tables, columns, and attributes.
	Introspect(ctx context.Context, params IntrospectParameters) (*Metadata, error)
	// SampleTable samples the table referenced by the TableMetadata meta
	// parameter and returns the sample as a slice of Sample. The parameters for
	// the sample, such as sample size, are passed via the params parameter (see
	// SampleParameters for more details). The returned sample result set
	// contains one Sample for each table row sampled. The length of the results
	// will be less than or equal to the sample size. If there are fewer results
	// than the specified sample size, it is because the table in question had a
	// row count less than the sample size. Prefer small sample sizes to limit
	// impact on the database.
	SampleTable(ctx context.Context, params SampleParameters) (Sample, error)
	// Ping is meant to be used as a general purpose connectivity test. It
	// should be invoked e.g. in the dry-run mode.
	Ping(ctx context.Context) error
	// Close is meant to be used as a general purpose cleanup. It should be
	// invoked when the Repository is no longer used.
	Close() error
}

// IntrospectParameters is a struct that holds the parameters for the Introspect
// method of the Repository interface.
type IntrospectParameters struct {
	// IncludePaths is a list of glob patterns that will be used to filter
	// the tables that will be introspected. If a table name matches any of
	// the patterns in this list, it will be included in the repository
	// metadata.
	IncludePaths []glob.Glob
	// ExcludePaths is a list of glob patterns that will be used to filter
	// the tables that will be introspected. If a table name matches any of
	// the patterns in this list, it will be excluded from the repository
	// metadata.
	ExcludePaths []glob.Glob
}

// Sample represents a sample of data from a database table.
type Sample struct {
	// TablePath is the full path of the data repository table that was sampled.
	// Each element corresponds to a component, in increasing order of
	// granularity (e.g. [database, schema, table]).
	TablePath []string
	// Results is the set of sample results. Each SampleResult is equivalent to
	// a database row, where the map key is the column name and the map value is
	// the column value.
	Results []SampleResult
}

// SampleParameters contains all parameters necessary to sample a table.
type SampleParameters struct {
	// Metadata is the metadata for the table to be sampled.
	Metadata *TableMetadata
	// SampleSize is the number of rows to sample from the table.
	SampleSize uint
	// Offset is the number of rows to skip before starting the sample.
	Offset uint
}

// SampleResult stores the results from a single database sample. It is
// equivalent to a database row, where the map key is the column name and the
// map value is the column value.
type SampleResult map[string]any
