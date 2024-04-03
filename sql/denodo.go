package sql

import (
	"context"
	"errors"
	"fmt"

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

// DenodoRepository is a Repository implementation for Denodo.
type DenodoRepository struct {
	// The majority of the Repository functionality is delegated to
	// a generic SQL repository instance.
	generic *GenericRepository
}

// DenodoRepository implements sql.Repository
var _ Repository = (*DenodoRepository)(nil)

// NewDenodoRepository is the constructor for sql.
func NewDenodoRepository(cfg RepoConfig) (*DenodoRepository, error) {
	pgCfg, err := parsePostgresConfig(cfg)
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
	generic, err := NewGenericRepository(RepoTypePostgres, cfg.Database, connStr, cfg.MaxOpenConns)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &DenodoRepository{generic: generic}, nil
}

// ListDatabases is left unimplemented for Denodo, because Denodo doesn't have
// the concept of databases.
func (r *DenodoRepository) ListDatabases(_ context.Context) ([]string, error) {
	return nil, errors.New("ListDatabases is not implemented for Denodo repositories")
}

// Introspect delegates introspection to GenericRepository. See
// Repository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *DenodoRepository) Introspect(ctx context.Context, params IntrospectParameters) (*Metadata, error) {
	return r.generic.IntrospectWithQuery(ctx, DenodoIntrospectQuery, params)
}

// SampleTable delegates sampling to GenericRepository, using a Denodo-specific
// table sample query. See Repository.SampleTable and
// GenericRepository.SampleTableWithQuery for more details.
func (r *DenodoRepository) SampleTable(
	ctx context.Context,
	params SampleParameters,
) (Sample, error) {
	// Denodo uses double-quotes to quote identifiers
	attrStr := params.Metadata.QuotedAttributeNamesString("\"")
	// The postgres driver is currently unable to properly send the
	// parameters of a prepared statement to Denodo. Therefore, instead of
	// building a prepared statement, we populate the query string before
	// sending it to the driver.
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s OFFSET %d ROWS LIMIT %d",
		attrStr, params.Metadata.Schema, params.Metadata.Name, params.Offset, params.SampleSize,
	)
	return r.generic.SampleTableWithQuery(ctx, query, params)
}

// Ping delegates the ping to GenericRepository. See Repository.Ping and
// GenericRepository.Ping for more details.
func (r *DenodoRepository) Ping(ctx context.Context) error {
	return r.generic.Ping(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *DenodoRepository) Close() error {
	return r.generic.Close()
}
