package denodo

import (
	"context"
	"errors"
	"fmt"

	"github.com/cyralinc/dmap/discovery/repository"
	"github.com/cyralinc/dmap/discovery/repository/genericsql"
	"github.com/cyralinc/dmap/discovery/repository/postgresql"

	"github.com/cyralinc/dmap/discovery/config"

	// Use PostgreSQL driver for Denodo
	_ "github.com/lib/pq"
)

const (
	RepoTypeDenodo = "denodo"

	// IntrospectQuery is the SQL query used to introspect the database. For
	// Denodo, the object hierarchy is (database > views). When querying
	// Denodo, the database corresponds to a schema, and the view corresponds
	// to a table (see SampleTable).
	IntrospectQuery = "SELECT " +
		"database_name AS table_schema, " +
		"view_name AS table_name, " +
		"column_name, " +
		"column_type_name AS data_type " +
		"FROM " +
		"CATALOG_VDP_METADATA_VIEWS()"
)

// Repository is a repository.Repository implementation for Denodo.
type Repository struct {
	// The majority of the repository.Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *genericsql.Repository
}

// Repository implements repository.Repository
var _ repository.Repository = (*Repository)(nil)

// NewRepository is the constructor for Repository.
func NewRepository(repoCfg config.RepoConfig) (*Repository, error) {
	pgCfg, err := postgresql.ParseConfig(repoCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse postgres config: %w", err)
	}
	if repoCfg.Database == "" {
		return nil, errors.New("database name is mandatory for Denodo repositories")
	}

	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s%s",
		repoCfg.User,
		repoCfg.Password,
		repoCfg.Host,
		repoCfg.Port,
		repoCfg.Database,
		pgCfg.ConnOptsStr,
	)

	sqlRepo, err := genericsql.NewRepository(
		repoCfg.Host,
		postgresql.RepoTypePostgres,
		repoCfg.Database,
		connStr,
		repoCfg.MaxOpenConns,
		repoCfg.IncludePaths,
		repoCfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}

	return &Repository{genericSqlRepo: sqlRepo}, nil
}

func (repo *Repository) ListDatabases(_ context.Context) ([]string, error) {
	return nil, errors.New("ListDatabases is not implemented for Denodo repositories")
}

func (repo *Repository) Introspect(ctx context.Context) (*repository.Metadata, error) {
	return repo.genericSqlRepo.IntrospectWithQuery(ctx, IntrospectQuery)
}

func (repo *Repository) SampleTable(
	ctx context.Context,
	meta *repository.TableMetadata,
	params repository.SampleParameters,
) (repository.Sample, error) {
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
	return repo.genericSqlRepo.SampleTableWithQuery(ctx, meta, query)
}

func (repo *Repository) Ping(ctx context.Context) error {
	return repo.genericSqlRepo.Ping(ctx)
}

func (repo *Repository) Close() error {
	return repo.genericSqlRepo.Close()
}

func init() {
	repository.Register(
		RepoTypeDenodo,
		func(_ context.Context, cfg config.RepoConfig) (repository.Repository, error) {
			return NewRepository(cfg)
		},
	)
}
