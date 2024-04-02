package sql

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/config"

	// Postgresql DB driver
	_ "github.com/lib/pq"
)

const (
	RepoTypePostgres = "postgres"

	PostgresDatabaseQuery = `
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
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *GenericRepository
}

// PostgresRepository implements Repository
var _ Repository = (*PostgresRepository)(nil)

// NewPostgresRepository creates a new PostgresRepository.
func NewPostgresRepository(cfg config.RepoConfig) (*PostgresRepository, error) {
	pgCfg, err := ParsePostgresConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("error parsing postgres config: %w", err)
	}
	database := cfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "postgres"
	}
	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		database,
		pgCfg.ConnOptsStr,
	)
	sqlRepo, err := NewGenericRepository(
		cfg.Host,
		RepoTypePostgres,
		cfg.Database,
		connStr,
		cfg.MaxOpenConns,
		cfg.IncludePaths,
		cfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &PostgresRepository{genericSqlRepo: sqlRepo}, nil
}

// ListDatabases returns a list of the names of all databases on the server by
// using a Postgres-specific database query. It delegates the actual work to
// GenericRepository.ListDatabasesWithQuery - see that method for more details.
func (r *PostgresRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.genericSqlRepo.ListDatabasesWithQuery(ctx, PostgresDatabaseQuery)
}

// Introspect delegates introspection to GenericRepository. See
// Repository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *PostgresRepository) Introspect(ctx context.Context) (*Metadata, error) {
	return r.genericSqlRepo.Introspect(ctx)
}

// SampleTable delegates sampling to GenericRepository, using a
// Postgres-specific table sample query. See Repository.SampleTable and
// GenericRepository.SampleTableWithQuery for more details.
func (r *PostgresRepository) SampleTable(
	ctx context.Context,
	meta *TableMetadata,
	params SampleParameters,
) (Sample, error) {
	// Postgres uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	// Postgres uses $x for placeholders
	query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT $1 OFFSET $2", attrStr, meta.Schema, meta.Name)
	return r.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.SampleSize, params.Offset)
}

// Ping delegates the ping to GenericRepository. See Repository.Ping and
// GenericRepository.Ping for more details.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.genericSqlRepo.Ping(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *PostgresRepository) Close() error {
	return r.genericSqlRepo.Close()
}

// PostgresConfig contains Postgres-specific configuration parameters.
type PostgresConfig struct {
	// ConnOptsStr is a string containing Postgres-specific connection options.
	ConnOptsStr string
}

// ParsePostgresConfig parses the Postgres-specific configuration parameters
// from the given config. The Postgres connection options are built from the
// config and stored in the ConnOptsStr field of the returned PostgresConfig.
func ParsePostgresConfig(cfg config.RepoConfig) (*PostgresConfig, error) {
	connOptsStr, err := config.BuildConnOptsStr(cfg)
	if err != nil {
		return nil, fmt.Errorf("error building connection options string: %w", err)
	}
	return &PostgresConfig{ConnOptsStr: connOptsStr}, nil
}
