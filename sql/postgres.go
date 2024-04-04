package sql

import (
	"context"
	"fmt"
	// Postgresql DB driver
	_ "github.com/lib/pq"
)

const (
	RepoTypePostgres = "postgres"
	postgresDatabaseQuery = `
SELECT 
	datname 
FROM
	pg_database
WHERE
	datistemplate = false
	AND datallowconn = true
	AND datname <> 'rdsadmin'
`
)

// PostgresRepository is a Repository implementation for Postgres databases.
type PostgresRepository struct {
	// The majority of the Repository functionality is delegated to
	// a generic SQL repository instance.
	generic *GenericRepository
}

// PostgresRepository implements Repository
var _ Repository = (*PostgresRepository)(nil)

// NewPostgresRepository creates a new PostgresRepository.
func NewPostgresRepository(cfg RepoConfig) (*PostgresRepository, error) {
	database := cfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "postgres"
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
	return &PostgresRepository{generic: generic}, nil
}

// ListDatabases returns a list of the names of all databases on the server by
// using a Postgres-specific database query. It delegates the actual work to
// GenericRepository.ListDatabasesWithQuery - see that method for more details.
func (r *PostgresRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.generic.ListDatabasesWithQuery(ctx, postgresDatabaseQuery)
}

// Introspect delegates introspection to GenericRepository. See
// Repository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *PostgresRepository) Introspect(ctx context.Context, params IntrospectParameters) (*Metadata, error) {
	return r.generic.Introspect(ctx, params)
}

// SampleTable delegates sampling to GenericRepository, using a
// Postgres-specific table sample query. See Repository.SampleTable and
// GenericRepository.SampleTableWithQuery for more details.
func (r *PostgresRepository) SampleTable(
	ctx context.Context,
	params SampleParameters,
) (Sample, error) {
	// Postgres uses double-quotes to quote identifiers
	attrStr := params.Metadata.QuotedAttributeNamesString("\"")
	// Postgres uses $x for placeholders
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s LIMIT $1 OFFSET $2",
		attrStr,
		params.Metadata.Schema,
		params.Metadata.Name,
	)
	return r.generic.SampleTableWithQuery(ctx, query, params)
}

// Ping delegates the ping to GenericRepository. See Repository.Ping and
// GenericRepository.Ping for more details.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.generic.Ping(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *PostgresRepository) Close() error {
	return r.generic.Close()
}
