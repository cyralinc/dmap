package sql

import (
	"context"
	"errors"
	"fmt"

	"github.com/cyralinc/dmap/discovery/config"

	// Oracle DB driver
	_ "github.com/sijms/go-ora/v2"
)

const (
	RepoTypeOracle        = "oracle"
	OracleIntrospectQuery = `
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
	configServiceName = "service-name"
)

// OracleRepository is a Repository implementation for Oracle databases.
type OracleRepository struct {
	// The majority of the OracleRepository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *GenericRepository
}

// OracleRepository implements Repository
var _ Repository = (*OracleRepository)(nil)

// NewOracleRepository creates a new Oracle repository.
func NewOracleRepository(cfg config.RepoConfig) (*OracleRepository, error) {
	oracleCfg, err := ParseOracleConfig(cfg)
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
	sqlRepo, err := NewGenericRepository(
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
	return &OracleRepository{genericSqlRepo: sqlRepo}, nil
}

// ListDatabases is left unimplemented for Oracle, because Oracle doesn't have
// the traditional concept of "databases". Note that Introspect already
// identifies all the accessible objects on the server.
func (r *OracleRepository) ListDatabases(_ context.Context) ([]string, error) {
	return nil, errors.New("ListDatabases is not implemented for Oracle repos")
}

// TODO: godoc -ccampo 2024-04-02
func (r *OracleRepository) Introspect(ctx context.Context) (*Metadata, error) {
	return r.genericSqlRepo.IntrospectWithQuery(ctx, OracleIntrospectQuery)
}

// TODO: godoc -ccampo 2024-04-02
func (r *OracleRepository) SampleTable(
	ctx context.Context,
	meta *TableMetadata,
	params SampleParameters,
) (Sample, error) {
	// Oracle uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	// Oracle uses :x for placeholders
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY",
		attrStr, meta.Schema, meta.Name,
	)
	return r.genericSqlRepo.SampleTableWithQuery(ctx, meta, query, params.Offset, params.SampleSize)
}

// Ping verifies the connection to Oracle database used by this Oracle
// Normally we would just delegate to the Ping method implemented by
// genericOracle However, that implementation executes a
// 'SELECT 1' query to test for connectivity, and Oracle being Oracle, does not
// like this. So instead, we defer to the native Ping method implemented by the
// Oracle DB driver.
func (r *OracleRepository) Ping(ctx context.Context) error {
	return r.genericSqlRepo.GetDb().PingContext(ctx)
}

// TODO: godoc -ccampo 2024-04-02
func (r *OracleRepository) Close() error {
	return r.genericSqlRepo.Close()
}

// TODO: godoc -ccampo 2024-04-02
type OracleConfig struct {
	ServiceName string
}

// TODO: godoc -ccampo 2024-04-02
func ParseOracleConfig(cfg config.RepoConfig) (*OracleConfig, error) {
	oracleCfg, err := config.FetchAdvancedConfigString(
		cfg,
		RepoTypeOracle,
		[]string{configServiceName},
	)
	if err != nil {
		return nil, fmt.Errorf("error fetching advanced oracle config: %w", err)
	}
	return &OracleConfig{ServiceName: oracleCfg[configServiceName]}, nil
}
