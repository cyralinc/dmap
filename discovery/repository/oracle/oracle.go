package oracle

import (
	"context"
	"errors"
	"fmt"

	"github.com/cyralinc/dmap/discovery/repository"
	"github.com/cyralinc/dmap/discovery/repository/genericsql"

	"github.com/cyralinc/dmap/discovery/config"

	// Oracle DB driver
	_ "github.com/sijms/go-ora/v2"
)

const (
	RepoTypeOracle  = "oracle"
	introspectQuery = `
WITH users AS (
  SELECT
    username
  FROM
    sys.all_users
  WHERE
    oracle_maintained = 'N' AND
    username <> 'RDSADMIN'
)
SELECT
  owner AS table_schema,
  table_name,
  column_name,
  data_type
FROM
  sys.all_tab_columns
INNER JOIN
  users
ON
  owner = users.username
`
)

type oracleRepository struct {
	// The majority of the repository.Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *genericsql.GenericSqlRepository
}

// *oracleRepository implements repository.Repository
var _ repository.Repository = (*oracleRepository)(nil)

func NewOracleRepository(_ context.Context, cfg config.RepoConfig) (repository.Repository, error) {
	oracleCfg, err := ParseConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse oracle config: %w", err)
	}

	connStr := fmt.Sprintf(
		`oracle://%s:%s@%s:%d/%s`,
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		oracleCfg.ServiceName,
	)

	sqlRepo, err := genericsql.NewGenericSqlRepository(
		cfg.Host,
		RepoTypeOracle,
		cfg.Database,
		connStr,
		cfg.MaxOpenConns,
		cfg.IncludePaths,
		cfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}

	return &oracleRepository{genericSqlRepo: sqlRepo}, nil
}

// ListDatabases is left unimplemented for Oracle, because Oracle doesn't have
// the traditional concept of "databases". Note that Introspect already
// identifies all the accessible objects on the server.
func (repo *oracleRepository) ListDatabases(_ context.Context) ([]string, error) {
	return nil, errors.New("ListDatabases is not implemented for Oracle repos")
}

func (repo *oracleRepository) Introspect(ctx context.Context) (*repository.Metadata, error) {
	return repo.genericSqlRepo.IntrospectWithQuery(ctx, introspectQuery)
}

func (repo *oracleRepository) SampleTable(
	ctx context.Context,
	meta *repository.TableMetadata,
	params repository.SampleParameters,
) (repository.Sample, error) {
	// Oracle uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	// Oracle uses :x for placeholders
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY",
		attrStr, meta.Schema, meta.Name,
	)
	return repo.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.Offset, params.SampleSize)
}

// Ping verifies the connection to Oracle database used by this Repository.
// Normally we would just delegate to the Ping method implemented by
// genericsql.GenericSqlRepository. However, that implementation executes a
// 'SELECT 1' query to test for connectivity, and Oracle being Oracle, does not
// like this. So instead, we defer to the native Ping method implemented by the
// Oracle sql.DB driver.
func (repo *oracleRepository) Ping(ctx context.Context) error {
	return repo.genericSqlRepo.GetDb().PingContext(ctx)
}

func (repo *oracleRepository) Close() error {
	return repo.genericSqlRepo.Close()
}

func init() {
	repository.Register(RepoTypeOracle, NewOracleRepository)
}
