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

// Repository is a repository.Repository implementation for Snowflake databases.
type Repository struct {
	// The majority of the repository.Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *genericsql.Repository
}

// Repository implements repository.Repository
var _ repository.Repository = (*Repository)(nil)

// NewRepository creates a new Snowflake repository.
func NewRepository(cfg config.RepoConfig) (*Repository, error) {
	snowflakeCfg, err := ParseConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("error when parsing snowflake config: %w", err)
	}
	database := cfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "SNOWFLAKE"
	}
	connStr := fmt.Sprintf(
		"%s:%s@%s/%s?role=%s&warehouse=%s",
		cfg.User,
		cfg.Password,
		snowflakeCfg.Account,
		cfg.Database,
		snowflakeCfg.Role,
		snowflakeCfg.Warehouse,
	)

	sqlRepo, err := genericsql.NewRepository(
		cfg.Host,
		RepoTypeSnowflake,
		database,
		connStr,
		cfg.MaxOpenConns,
		cfg.IncludePaths,
		cfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}

	return &Repository{genericSqlRepo: sqlRepo}, nil
}

func (repo *Repository) ListDatabases(ctx context.Context) ([]string, error) {
	return repo.genericSqlRepo.ListDatabasesWithQuery(ctx, DatabaseQuery)
}

func (repo *Repository) Introspect(ctx context.Context) (*repository.Metadata, error) {
	return repo.genericSqlRepo.Introspect(ctx)
}

func (repo *Repository) SampleTable(
	ctx context.Context,
	meta *repository.TableMetadata,
	params repository.SampleParameters,
) (repository.Sample, error) {
	return repo.genericSqlRepo.SampleTable(ctx, meta, params)
}

func (repo *Repository) Ping(ctx context.Context) error {
	return repo.genericSqlRepo.Ping(ctx)
}

func (repo *Repository) Close() error {
	return repo.genericSqlRepo.Close()
}

func init() {
	repository.Register(
		RepoTypeSnowflake,
		func(ctx context.Context, cfg config.RepoConfig) (repository.Repository, error) {
			return NewRepository(cfg)
		},
	)
}
