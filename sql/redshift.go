package sql

import (
	"context"
	"fmt"

	// Use PostgreSQL DB driver for Redshift
	_ "github.com/lib/pq"
)

const (
	RepoTypeRedshift = "redshift"
)

// RedshiftRepository is a Repository implementation for Redshift databases.
type RedshiftRepository struct {
	// The majority of the RedshiftRepository functionality is delegated to
	// a generic SQL repository instance.
	generic *GenericRepository
}

// RedshiftRepository implements Repository
var _ Repository = (*RedshiftRepository)(nil)

// NewRedshiftRepository creates a new RedshiftRepository.
func NewRedshiftRepository(cfg RepoConfig) (*RedshiftRepository, error) {
	database := cfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "dev"
	}
	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		database,
	)
	generic, err := NewGenericRepository(RepoTypePostgres, cfg.Database, connStr, cfg.MaxOpenConns)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &RedshiftRepository{generic: generic}, nil
}

// ListDatabases returns a list of the names of all databases on the server by
// using a Redshift-specific database query. It delegates the actual work to
// GenericRepository.ListDatabasesWithQuery - see that method for more details.
func (r *RedshiftRepository) ListDatabases(ctx context.Context) ([]string, error) {
	// Redshift and Postgres use the same query to list the server databases.
	return r.generic.ListDatabasesWithQuery(ctx, postgresDatabaseQuery)
}

// Introspect delegates introspection to GenericRepository. See
// Repository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *RedshiftRepository) Introspect(ctx context.Context, params IntrospectParameters) (*Metadata, error) {
	return r.generic.Introspect(ctx, params)
}

// SampleTable delegates sampling to GenericRepository, using a
// Redshift-specific table sample query. See Repository.SampleTable and
// GenericRepository.SampleTableWithQuery for more details.
func (r *RedshiftRepository) SampleTable(
	ctx context.Context,
	params SampleParameters,
) (Sample, error) {
	// Redshift uses double-quotes to quote identifiers
	attrStr := params.Metadata.QuotedAttributeNamesString("\"")
	// Redshift uses $x for placeholders
	query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT $1 OFFSET $2", attrStr, params.Metadata.Schema, params.Metadata.Name)
	return r.generic.SampleTableWithQuery(ctx, query, params)
}

// Ping delegates the ping to GenericRepository. See Repository.Ping and
// GenericRepository.Ping for more details.
func (r *RedshiftRepository) Ping(ctx context.Context) error {
	return r.generic.Ping(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *RedshiftRepository) Close() error {
	return r.generic.Close()
}
