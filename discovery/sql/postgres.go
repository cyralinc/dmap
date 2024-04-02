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

// TODO: godoc -ccampo 2024-04-02
func (r *PostgresRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.genericSqlRepo.ListDatabasesWithQuery(ctx, PostgresDatabaseQuery)
}

// TODO: godoc -ccampo 2024-04-02
func (r *PostgresRepository) Introspect(ctx context.Context) (*Metadata, error) {
	return r.genericSqlRepo.Introspect(ctx)
}

// TODO: godoc -ccampo 2024-04-02
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

// TODO: godoc -ccampo 2024-04-02
func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.genericSqlRepo.Ping(ctx)
}

// TODO: godoc -ccampo 2024-04-02
func (r *PostgresRepository) Close() error {
	return r.genericSqlRepo.Close()
}

// TODO: godoc -ccampo 2024-04-02
type PostgresConfig struct {
	ConnOptsStr string
}

// ParsePostgresConfig produces a config structure with Postgres-specific
// parameters found in the repo config.
func ParsePostgresConfig(cfg config.RepoConfig) (*PostgresConfig, error) {
	connOptsStr, err := config.BuildConnOptsStr(cfg)
	if err != nil {
		return nil, err
	}

	return &PostgresConfig{
		ConnOptsStr: connOptsStr,
	}, nil
}
