package mysql

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/repository"
	"github.com/cyralinc/dmap/discovery/repository/genericsql"

	"github.com/cyralinc/dmap/discovery/config"

	// MySQL DB driver
	_ "github.com/go-sql-driver/mysql"
)

const (
	RepoTypeMysql = "mysql"
	DatabaseQuery = `
SELECT 
    schema_name
FROM 
    information_schema.schemata
WHERE
    schema_name <> 'information_schema'
    AND schema_name <> 'performance_schema'
    AND schema_name <> 'sys'
`
)

type mySqlRepository struct {
	// The majority of the repository.Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *genericsql.GenericSqlRepository
}

// *mySqlRepository implements repository.Repository
var _ repository.Repository = (*mySqlRepository)(nil)

func NewMySQLRepository(_ context.Context, cfg config.RepoConfig) (repository.Repository, error) {
	connStr := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		// This can be an empty string. See:
		// https://github.com/go-sql-driver/mysql#dsn-data-source-name
		cfg.Database,
	)

	sqlRepo, err := genericsql.NewGenericSqlRepository(
		cfg.Host,
		RepoTypeMysql,
		cfg.Database,
		connStr,
		cfg.MaxOpenConns,
		cfg.IncludePaths,
		cfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}

	return &mySqlRepository{genericSqlRepo: sqlRepo}, nil
}

func (repo *mySqlRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return repo.genericSqlRepo.ListDatabasesWithQuery(ctx, DatabaseQuery)
}

func (repo *mySqlRepository) Introspect(ctx context.Context) (*repository.Metadata, error) {
	return repo.genericSqlRepo.Introspect(ctx)
}

func (repo *mySqlRepository) SampleTable(
	ctx context.Context,
	meta *repository.TableMetadata,
	params repository.SampleParameters,
) (repository.Sample, error) {
	// MySQL uses backticks to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("`")
	// The generic select/limit/offset query and ? placeholders work fine with MySQL
	query := fmt.Sprintf(genericsql.SampleQueryTemplate, attrStr, meta.Schema, meta.Name)
	return repo.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.SampleSize, params.Offset)
}

func (repo *mySqlRepository) Ping(ctx context.Context) error {
	return repo.genericSqlRepo.Ping(ctx)
}

func (repo *mySqlRepository) Close() error {
	return repo.genericSqlRepo.Close()
}

func init() {
	repository.Register(RepoTypeMysql, NewMySQLRepository)
}
