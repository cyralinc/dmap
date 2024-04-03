package sql

import (
	"context"
	"fmt"

	// MySQL DB driver
	_ "github.com/go-sql-driver/mysql"
)

const (
	RepoTypeMysql      = "mysql"
	MySqlDatabaseQuery = `
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

// MySqlRepository is a Repository implementation for MySQL databases.
type MySqlRepository struct {
	// The majority of the Repository functionality is delegated to
	// a generic SQL repository instance.
	generic *GenericRepository
}

// MySqlRepository implements Repository
var _ Repository = (*MySqlRepository)(nil)

// NewMySqlRepository creates a new MySQL sql.
func NewMySqlRepository(cfg RepoConfig) (*MySqlRepository, error) {
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
	generic, err := NewGenericRepository(RepoTypeMysql, cfg.Database, connStr, cfg.MaxOpenConns)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &MySqlRepository{generic: generic}, nil
}

// ListDatabases returns a list of the names of all databases on the server by
// using a MySQL-specific database query. It delegates the actual work to
// GenericRepository.ListDatabasesWithQuery - see that method for more details.
func (r *MySqlRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.generic.ListDatabasesWithQuery(ctx, MySqlDatabaseQuery)
}

// Introspect delegates introspection to GenericRepository. See
// Repository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *MySqlRepository) Introspect(ctx context.Context, params IntrospectParameters) (*Metadata, error) {
	return r.generic.Introspect(ctx, params)
}

// SampleTable delegates sampling to GenericRepository, using a MySQL-specific
// table sample query. See Repository.SampleTable and
// GenericRepository.SampleTableWithQuery for more details.
func (r *MySqlRepository) SampleTable(
	ctx context.Context,
	params SampleParameters,
) (Sample, error) {
	// MySQL uses backticks to quote identifiers.
	attrStr := params.Metadata.QuotedAttributeNamesString("`")
	// The generic select/limit/offset query and ? placeholders work fine with
	// MySQL.
	query := fmt.Sprintf(GenericSampleQueryTemplate, attrStr, params.Metadata.Schema, params.Metadata.Name)
	return r.generic.SampleTableWithQuery(ctx, query, params)
}

// Ping delegates the ping to GenericRepository. See Repository.Ping and
// GenericRepository.Ping for more details.
func (r *MySqlRepository) Ping(ctx context.Context) error {
	return r.generic.Ping(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *MySqlRepository) Close() error {
	return r.generic.Close()
}
