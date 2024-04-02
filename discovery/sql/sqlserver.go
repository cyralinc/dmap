package sql

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/config"

	// SQL Server DB driver
	_ "github.com/denisenkom/go-mssqldb"
)

const (
	RepoTypeSqlServer = "sqlserver"
	// SqlServerSampleQueryTemplate is the string template for the SQL query used to
	// sample a SQL Server database. SQL Server doesn't support limit/offset, so
	// we use top. It also uses the @ symbol for statement parameter
	// placeholders. It is intended to be templated by the database name to
	// query.
	SqlServerSampleQueryTemplate = `SELECT TOP (@p1) %s FROM "%s"."%s"`
	// SqlServerDatabaseQuery is the query to list all the databases on the server, minus
	// the system default databases 'model' and 'tempdb'.
	SqlServerDatabaseQuery = "SELECT name FROM sys.databases WHERE name != 'model' AND name != 'tempdb'"
)

// SqlServerRepository is a Repository implementation for MS SQL Server
// databases.
type SqlServerRepository struct {
	// The majority of the Repository functionality is delegated to a generic
	// SQL repository instance (genericSqlRepo).
	genericSqlRepo *GenericRepository
}

// SqlServerRepository implements Repository
var _ Repository = (*SqlServerRepository)(nil)

// NewSqlServerRepository creates a new MS SQL Server sql.
func NewSqlServerRepository(cfg config.RepoConfig) (*SqlServerRepository, error) {
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
	genericSqlRepo, err := NewGenericRepository(
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

// TODO: godoc -ccampo 2024-04-02
func (r *SqlServerRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.genericSqlRepo.ListDatabasesWithQuery(ctx, SqlServerDatabaseQuery)
}

// TODO: godoc -ccampo 2024-04-02
func (r *SqlServerRepository) Introspect(ctx context.Context) (*Metadata, error) {
	return r.genericSqlRepo.Introspect(ctx)
}

// TODO: godoc -ccampo 2024-04-02
func (r *SqlServerRepository) SampleTable(
	ctx context.Context,
	meta *TableMetadata,
	params SampleParameters,
) (Sample, error) {
	// Sqlserver uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	query := fmt.Sprintf(SqlServerSampleQueryTemplate, attrStr, meta.Schema, meta.Name)
	return r.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.SampleSize)
}

// TODO: godoc -ccampo 2024-04-02
func (r *SqlServerRepository) Ping(ctx context.Context) error {
	return r.genericSqlRepo.Ping(ctx)
}

// TODO: godoc -ccampo 2024-04-02
func (r *SqlServerRepository) Close() error {
	return r.genericSqlRepo.Close()
}
