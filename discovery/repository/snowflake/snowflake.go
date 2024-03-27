package snowflake

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/repository"
	"github.com/cyralinc/dmap/discovery/repository/genericsql"

	"github.com/cyralinc/dmap/discovery/config"

	// Snowflake DB driver
	_ "github.com/snowflakedb/gosnowflake"
)

const (
	RepoTypeSnowflake = "snowflake"

	DatabaseQuery = `
SELECT 
    DATABASE_NAME 
FROM 
    INFORMATION_SCHEMA.DATABASES 
WHERE 
    IS_TRANSIENT = 'NO'
`
)

type snowflakeRepository struct {
	// The majority of the repository.Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *genericsql.GenericSqlRepository
}

// *snowflakeRepository implements repository.Repository
var _ repository.Repository = (*snowflakeRepository)(nil)

func NewSnowflakeRepository(_ context.Context, repoCfg config.RepoConfig) (repository.Repository, error) {
	snowflakeCfg, err := ParseConfig(repoCfg)
	if err != nil {
		return nil, fmt.Errorf("error when parsing snowflake config: %w", err)
	}
	database := repoCfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "SNOWFLAKE"
	}
	connStr := fmt.Sprintf(
		"%s:%s@%s/%s?role=%s&warehouse=%s",
		repoCfg.User,
		repoCfg.Password,
		snowflakeCfg.Account,
		repoCfg.Database,
		snowflakeCfg.Role,
		snowflakeCfg.Warehouse,
	)

	sqlRepo, err := genericsql.NewGenericSqlRepository(
		repoCfg.Host,
		RepoTypeSnowflake,
		repoCfg.Database,
		connStr,
		repoCfg.MaxOpenConns,
		repoCfg.IncludePaths,
		repoCfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}

	return &snowflakeRepository{genericSqlRepo: sqlRepo}, nil
}

func (repo *snowflakeRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return repo.genericSqlRepo.ListDatabasesWithQuery(ctx, DatabaseQuery)
}

func (repo *snowflakeRepository) Introspect(ctx context.Context) (*repository.Metadata, error) {
	return repo.genericSqlRepo.Introspect(ctx)
}

func (repo *snowflakeRepository) SampleTable(
	ctx context.Context,
	meta *repository.TableMetadata,
	params repository.SampleParameters,
) (repository.Sample, error) {
	return repo.genericSqlRepo.SampleTable(ctx, meta, params)
}

func (repo *snowflakeRepository) Ping(ctx context.Context) error {
	return repo.genericSqlRepo.Ping(ctx)
}

func (repo *snowflakeRepository) Close() error {
	return repo.genericSqlRepo.Close()
}

func init() {
	repository.Register(RepoTypeSnowflake, NewSnowflakeRepository)
}
