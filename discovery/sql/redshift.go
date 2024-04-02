package sql

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/config"

	// Use PostgreSQL DB driver for Redshift
	_ "github.com/lib/pq"
)

const (
	RepoTypeRedshift = "redshift"
)

// RedshiftRepository is a Repository implementation for Redshift databases.
type RedshiftRepository struct {
	// The majority of the RedshiftRepository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *GenericRepository
}

// RedshiftRepository implements Repository
var _ Repository = (*RedshiftRepository)(nil)

// NewRedshiftRepository creates a new RedshiftRepository.
func NewRedshiftRepository(cfg config.RepoConfig) (*RedshiftRepository, error) {
	pgCfg, err := ParsePostgresConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse postgres config: %w", err)
	}
	database := cfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "dev"
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
	return &RedshiftRepository{genericSqlRepo: sqlRepo}, nil
}

// TODO: godoc -ccampo 2024-04-02
func (r *RedshiftRepository) ListDatabases(ctx context.Context) ([]string, error) {
	// Redshift and Postgres use the same query to list the server databases.
	return r.genericSqlRepo.ListDatabasesWithQuery(ctx, PostgresDatabaseQuery)
}

// TODO: godoc -ccampo 2024-04-02
func (r *RedshiftRepository) Introspect(ctx context.Context) (*Metadata, error) {
	return r.genericSqlRepo.Introspect(ctx)
}

// TODO: godoc -ccampo 2024-04-02
func (r *RedshiftRepository) SampleTable(
	ctx context.Context,
	meta *TableMetadata,
	params SampleParameters,
) (Sample, error) {
	// Redshift uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	// Redshift uses $x for placeholders
	query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT $1 OFFSET $2", attrStr, meta.Schema, meta.Name)
	return r.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.SampleSize, params.Offset)
}

// TODO: godoc -ccampo 2024-04-02
func (r *RedshiftRepository) Ping(ctx context.Context) error {
	return r.genericSqlRepo.Ping(ctx)
}

// TODO: godoc -ccampo 2024-04-02
func (r *RedshiftRepository) Close() error {
	return r.genericSqlRepo.Close()
}
