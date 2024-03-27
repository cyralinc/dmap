package sqlserver

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/repository"
	"github.com/cyralinc/dmap/discovery/repository/genericsql"

	"github.com/cyralinc/dmap/discovery/config"

	// SQL Server DB driver
	_ "github.com/denisenkom/go-mssqldb"
)

const (
	RepoTypeSqlServer = "sqlserver"

	// SampleQueryTemplate is the string template for the SQL query used to
	// sample a SQL Server database. SQL Server doesn't support limit/offset, so
	// we use top. It also uses the @ symbol for statement parameter
	// placeholders. It is intended to be templated by the database name to
	// query.
	SampleQueryTemplate = `SELECT TOP (@p1) %s FROM "%s"."%s"`

	// DatabaseQuery is the query to list all the databases on the server, minus
	// the system default databases 'model' and 'tempdb'.
	DatabaseQuery = "SELECT name FROM sys.databases WHERE name != 'model' AND name != 'tempdb'"
)

type SqlServerRepository struct {
	// The majority of the repository.Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *genericsql.GenericSqlRepository
}

// *SqlServerRepository implements repository.Repository
var _ repository.Repository = (*SqlServerRepository)(nil)

func NewSqlServerRepository(_ context.Context, cfg config.RepoConfig) (*SqlServerRepository, error) {
	connStr := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
	)

	// The database name is optional for MS SQL Server.
	if cfg.Database != "" {
		connStr = fmt.Sprintf(connStr+"?database=%s", cfg.Database)
	}

	genericSqlRepo, err := genericsql.NewGenericSqlRepository(
		cfg.Host,
		RepoTypeSqlServer,
		cfg.Database,
		connStr,
		cfg.MaxOpenConns,
		cfg.IncludePaths,
		cfg.IncludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}

	return &SqlServerRepository{genericSqlRepo: genericSqlRepo}, nil
}

func (repo *SqlServerRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return repo.genericSqlRepo.ListDatabasesWithQuery(ctx, DatabaseQuery)
}

func (repo *SqlServerRepository) Introspect(ctx context.Context) (*repository.Metadata, error) {
	return repo.genericSqlRepo.Introspect(ctx)
}

func (repo *SqlServerRepository) SampleTable(
	ctx context.Context,
	meta *repository.TableMetadata,
	params repository.SampleParameters,
) (repository.Sample, error) {
	// Sqlserver uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	query := fmt.Sprintf(SampleQueryTemplate, attrStr, meta.Schema, meta.Name)
	return repo.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.SampleSize)
}

func (repo *SqlServerRepository) Ping(ctx context.Context) error {
	return repo.genericSqlRepo.Ping(ctx)
}

func (repo *SqlServerRepository) Close() error {
	return repo.genericSqlRepo.Close()
}

func init() {
	repository.Register(
		RepoTypeSqlServer,
		func(ctx context.Context, cfg config.RepoConfig) (repository.Repository, error) {
			return NewSqlServerRepository(ctx, cfg)
		},
	)
}
