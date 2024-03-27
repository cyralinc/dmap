package postgresql

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/repository"
	"github.com/cyralinc/dmap/discovery/repository/genericsql"

	"github.com/cyralinc/dmap/discovery/config"
	// Postgresql DB driver
	_ "github.com/lib/pq"
)

const (
	RepoTypePostgres = "postgres"

	DatabaseQuery = `
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

type postgresqlRepository struct {
	// The majority of the repository.Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *genericsql.GenericSqlRepository
}

// *postgresqlRepository implements repository.Repository
var _ repository.Repository = (*postgresqlRepository)(nil)

func NewPostgresqlRepository(_ context.Context, repoCfg config.RepoConfig) (repository.Repository, error) {
	pgCfg, err := ParseConfig(repoCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse postgresql config: %w", err)
	}
	database := repoCfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "postgres"
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
		RepoTypePostgres,
		repoCfg.Database,
		connStr,
		repoCfg.MaxOpenConns,
		repoCfg.IncludePaths,
		repoCfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &postgresqlRepository{genericSqlRepo: sqlRepo}, nil
}

func (repo *postgresqlRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return repo.genericSqlRepo.ListDatabasesWithQuery(ctx, DatabaseQuery)
}

func (repo *postgresqlRepository) Introspect(ctx context.Context) (*repository.Metadata, error) {
	return repo.genericSqlRepo.Introspect(ctx)
}

func (repo *postgresqlRepository) SampleTable(
	ctx context.Context,
	meta *repository.TableMetadata,
	params repository.SampleParameters,
) (repository.Sample, error) {
	// PostgreSQL uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	// PostgreSQL uses $x for placeholders
	query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT $1 OFFSET $2", attrStr, meta.Schema, meta.Name)
	return repo.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.SampleSize, params.Offset)
}

func (repo *postgresqlRepository) Ping(ctx context.Context) error {
	return repo.genericSqlRepo.Ping(ctx)
}

func (repo *postgresqlRepository) Close() error {
	return repo.genericSqlRepo.Close()
}

func init() {
	repository.Register(RepoTypePostgres, NewPostgresqlRepository)
}
