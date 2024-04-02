package sql

import (
	"context"
	"errors"
	"fmt"

	"github.com/cyralinc/dmap/discovery/config"

	// Use PostgreSQL driver for Denodo
	_ "github.com/lib/pq"
)

const (
	RepoTypeDenodo = "denodo"
	// DenodoIntrospectQuery is the SQL query used to introspect the database. For
	// Denodo, the object hierarchy is (database > views). When querying
	// Denodo, the database corresponds to a schema, and the view corresponds
	// to a table (see SampleTable).
	DenodoIntrospectQuery = "SELECT " +
		"database_name AS table_schema, " +
		"view_name AS table_name, " +
		"column_name, " +
		"column_type_name AS data_type " +
		"FROM " +
		"CATALOG_VDP_METADATA_VIEWS()"
)

// DenodoRepository is a sql.Repository implementation for Denodo.
type DenodoRepository struct {
	// The majority of the sql.Repository functionality is delegated to
	// a generic SQL repository instance.
	genericSqlRepo *GenericRepository
}

// DenodoRepository implements sql.Repository
var _ Repository = (*DenodoRepository)(nil)

// NewDenodoRepository is the constructor for sql.
func NewDenodoRepository(cfg config.RepoConfig) (*DenodoRepository, error) {
	pgCfg, err := ParsePostgresConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse postgres config: %w", err)
	}
	if cfg.Database == "" {
		return nil, errors.New("database name is mandatory for Denodo repositories")
	}
	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
		pgCfg.ConnOptsStr,
	)
	sqlRepo, err := NewGenericRepository(
		cfg.Host,
		RepoTypePostgres,
		cfg.Database,
		connStr,
		cfg.MaxOpenConns,
		cfg.IncludePaths,
		cfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &DenodoRepository{genericSqlRepo: sqlRepo}, nil
}

// TODO: godoc -ccampo 2024-04-02
func (r *DenodoRepository) ListDatabases(_ context.Context) ([]string, error) {
	return nil, errors.New("ListDatabases is not implemented for Denodo repositories")
}

// TODO: godoc -ccampo 2024-04-02
func (r *DenodoRepository) Introspect(ctx context.Context) (*Metadata, error) {
	return r.genericSqlRepo.IntrospectWithQuery(ctx, DenodoIntrospectQuery)
}

// TODO: godoc -ccampo 2024-04-02
func (r *DenodoRepository) SampleTable(
	ctx context.Context,
	meta *TableMetadata,
	params SampleParameters,
) (Sample, error) {
	// Denodo uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	// The postgres driver is currently unable to properly send the
	// parameters of a prepared statement to Denodo. Therefore, instead of
	// building a prepared statement, we populate the query string before
	// sending it to the driver.
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s OFFSET %d ROWS LIMIT %d",
		attrStr, meta.Schema, meta.Name, params.Offset, params.SampleSize,
	)
	return r.genericSqlRepo.SampleTableWithQuery(ctx, meta, query)
}

// TODO: godoc -ccampo 2024-04-02
func (r *DenodoRepository) Ping(ctx context.Context) error {
	return r.genericSqlRepo.Ping(ctx)
}

// TODO: godoc -ccampo 2024-04-02
func (r *DenodoRepository) Close() error {
	return r.genericSqlRepo.Close()
}
