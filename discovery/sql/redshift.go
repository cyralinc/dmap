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

// ListDatabases returns a list of the names of all databases on the server by
// using a Redshift-specific database query. It delegates the actual work to
// GenericRepository.ListDatabasesWithQuery - see that method for more details.
func (r *RedshiftRepository) ListDatabases(ctx context.Context) ([]string, error) {
	// Redshift and Postgres use the same query to list the server databases.
	return r.genericSqlRepo.ListDatabasesWithQuery(ctx, PostgresDatabaseQuery)
}

// Introspect delegates introspection to GenericRepository. See
// Repository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *RedshiftRepository) Introspect(ctx context.Context) (*Metadata, error) {
	return r.genericSqlRepo.Introspect(ctx)
}

// SampleTable delegates sampling to GenericRepository, using a
// Redshift-specific table sample query. See Repository.SampleTable and
// GenericRepository.SampleTableWithQuery for more details.
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

// Ping delegates the ping to GenericRepository. See Repository.Ping and
// GenericRepository.Ping for more details.
func (r *RedshiftRepository) Ping(ctx context.Context) error {
	return r.genericSqlRepo.Ping(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *RedshiftRepository) Close() error {
	return r.genericSqlRepo.Close()
}
