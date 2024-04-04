package sql

import (
	"context"
	"fmt"

	// SQL Server DB driver
	_ "github.com/denisenkom/go-mssqldb"
)

const (
	RepoTypeSqlServer = "sqlserver"
	// sqlServerSampleQueryTemplate is the string template for the SQL query used to
	// sample a SQL Server database. SQL Server doesn't support limit/offset, so
	// we use top. It also uses the @ symbol for statement parameter
	// placeholders. It is intended to be templated by the database name to
	// query.
	sqlServerSampleQueryTemplate = `SELECT TOP (@p1) %s FROM "%s"."%s"`
	// sqlServerDatabaseQuery is the query to list all the databases on the server, minus
	// the system default databases 'model' and 'tempdb'.
	sqlServerDatabaseQuery = "SELECT name FROM sys.databases WHERE name != 'model' AND name != 'tempdb'"
)

// SqlServerRepository is a Repository implementation for MS SQL Server
// databases.
type SqlServerRepository struct {
	// The majority of the Repository functionality is delegated to a generic
	// SQL repository instance.
	generic *GenericRepository
}

// SqlServerRepository implements Repository
var _ Repository = (*SqlServerRepository)(nil)

// NewSqlServerRepository creates a new MS SQL Server sql.
func NewSqlServerRepository(cfg RepoConfig) (*SqlServerRepository, error) {
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
	generic, err := NewGenericRepository(RepoTypeSqlServer, cfg.Database, connStr, cfg.MaxOpenConns)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &SqlServerRepository{generic: generic}, nil
}

// ListDatabases returns a list of the names of all databases on the server by
// using a SQL Server-specific database query. It delegates the actual work to
// GenericRepository.ListDatabasesWithQuery - see that method for more details.
func (r *SqlServerRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.generic.ListDatabasesWithQuery(ctx, sqlServerDatabaseQuery)
}

// Introspect delegates introspection to GenericRepository. See
// Repository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *SqlServerRepository) Introspect(ctx context.Context, params IntrospectParameters) (*Metadata, error) {
	return r.generic.Introspect(ctx, params)
}

// SampleTable delegates sampling to GenericRepository, using a
// SQL Server-specific table sample query. See Repository.SampleTable and
// GenericRepository.SampleTableWithQuery for more details.
func (r *SqlServerRepository) SampleTable(
	ctx context.Context,
	params SampleParameters,
) (Sample, error) {
	// Sqlserver uses double-quotes to quote identifiers
	attrStr := params.Metadata.QuotedAttributeNamesString("\"")
	query := fmt.Sprintf(sqlServerSampleQueryTemplate, attrStr, params.Metadata.Schema, params.Metadata.Name)
	return r.generic.SampleTableWithQuery(ctx, query, params)
}

// Ping delegates the ping to GenericRepository. See Repository.Ping and
// GenericRepository.Ping for more details.
func (r *SqlServerRepository) Ping(ctx context.Context) error {
	return r.generic.Ping(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *SqlServerRepository) Close() error {
	return r.generic.Close()
}
