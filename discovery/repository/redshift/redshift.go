package redshift

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/config"
	"github.com/cyralinc/dmap/discovery/repository"
	"github.com/cyralinc/dmap/discovery/repository/genericsql"
	"github.com/cyralinc/dmap/discovery/repository/postgresql"

	// Use PostgreSQL DB driver for Redshift
	_ "github.com/lib/pq"
)

const (
	RepoTypeRedshift = "redshift"
)

type redshiftRepository struct {
	// The majority of the repository.Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *genericsql.GenericSqlRepository
}

// *redshiftRepository implements repository.Repository
var _ repository.Repository = (*redshiftRepository)(nil)

func NewRedshiftRepository(_ context.Context, repoCfg config.RepoConfig) (repository.Repository, error) {
	pgCfg, err := postgresql.ParseConfig(repoCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse postgres config: %w", err)
	}
	database := repoCfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "dev"
	}
	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s%s",
		repoCfg.User,
		repoCfg.Password,
		repoCfg.Host,
		repoCfg.Port,
		database,
		pgCfg.ConnOptsStr,
	)
	sqlRepo, err := genericsql.NewGenericSqlRepository(
		repoCfg.Host,
		postgresql.RepoTypePostgres,
		repoCfg.Database,
		connStr,
		repoCfg.MaxOpenConns,
		repoCfg.IncludePaths,
		repoCfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &redshiftRepository{genericSqlRepo: sqlRepo}, nil
}

func (repo *redshiftRepository) ListDatabases(ctx context.Context) ([]string, error) {
	// Redshift and Postgres use the same query to list the server databases.
	return repo.genericSqlRepo.ListDatabasesWithQuery(ctx, postgresql.DatabaseQuery)
}

func (repo *redshiftRepository) Introspect(ctx context.Context) (*repository.Metadata, error) {
	return repo.genericSqlRepo.Introspect(ctx)
}

func (repo *redshiftRepository) SampleTable(
	ctx context.Context,
	meta *repository.TableMetadata,
	params repository.SampleParameters,
) (repository.Sample, error) {
	// Redshift uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	// Redshift uses $x for placeholders
	query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT $1 OFFSET $2", attrStr, meta.Schema, meta.Name)
	return repo.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.SampleSize, params.Offset)
}

func (repo *redshiftRepository) Ping(ctx context.Context) error {
	return repo.genericSqlRepo.Ping(ctx)
}

func (repo *redshiftRepository) Close() error {
	return repo.genericSqlRepo.Close()
}

func init() {
	repository.Register(RepoTypeRedshift, NewRedshiftRepository)
}
