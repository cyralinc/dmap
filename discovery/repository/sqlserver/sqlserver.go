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

// Repository is a repository.Repository implementation for MS SQL Server
// databases.
type Repository struct {
	// The majority of the repository.Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *genericsql.Repository
}

// Repository implements repository.Repository
var _ repository.Repository = (*Repository)(nil)

// NewRepository creates a new MS SQL Server repository.
func NewRepository(cfg config.RepoConfig) (*Repository, error) {
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

	genericSqlRepo, err := genericsql.NewRepository(
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

	return &Repository{genericSqlRepo: genericSqlRepo}, nil
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
	// Sqlserver uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	query := fmt.Sprintf(SampleQueryTemplate, attrStr, meta.Schema, meta.Name)
	return repo.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.SampleSize)
}

func (repo *Repository) Ping(ctx context.Context) error {
	return repo.genericSqlRepo.Ping(ctx)
}

func (repo *Repository) Close() error {
	return repo.genericSqlRepo.Close()
}

func init() {
	repository.Register(
		RepoTypeSqlServer,
		func(_ context.Context, cfg config.RepoConfig) (repository.Repository, error) {
			return NewRepository(cfg)
		},
	)
}
