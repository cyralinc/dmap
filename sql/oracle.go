package sql

import (
	"context"
	"errors"
	"fmt"

	// Oracle DB driver
	_ "github.com/sijms/go-ora/v2"
)

const (
	RepoTypeOracle        = "oracle"
	oracleIntrospectQuery = `
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
	// a generic SQL repository instance.
	generic *GenericRepository
}

// OracleRepository implements Repository
var _ Repository = (*OracleRepository)(nil)

// NewOracleRepository creates a new Oracle repository.
func NewOracleRepository(cfg RepoConfig) (*OracleRepository, error) {
	oracleCfg, err := NewOracleConfigFromMap(cfg.Advanced)
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
	generic, err := NewGenericRepository(RepoTypeOracle, cfg.Database, connStr, cfg.MaxOpenConns)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &OracleRepository{generic: generic}, nil
}

// ListDatabases is left unimplemented for Oracle, because Oracle doesn't have
// the traditional concept of "databases". Note that Introspect already
// identifies all the accessible objects on the server.
func (r *OracleRepository) ListDatabases(_ context.Context) ([]string, error) {
	return nil, errors.New("ListDatabases is not implemented for Oracle repos")
}

// Introspect delegates introspection to GenericRepository, using an
// Oracle-specific introspection query. See Repository.Introspect and
// GenericRepository.IntrospectWithQuery for more details.
func (r *OracleRepository) Introspect(ctx context.Context, params IntrospectParameters) (*Metadata, error) {
	return r.generic.IntrospectWithQuery(ctx, oracleIntrospectQuery, params)
}

// SampleTable delegates sampling to GenericRepository, using an Oracle-specific
// table sample query. See Repository.SampleTable and
// GenericRepository.SampleTableWithQuery for more details.
func (r *OracleRepository) SampleTable(
	ctx context.Context,
	params SampleParameters,
) (Sample, error) {
	// Oracle uses double-quotes to quote identifiers.
	attrStr := params.Metadata.QuotedAttributeNamesString("\"")
	// Oracle uses :x for placeholders.
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY",
		attrStr, params.Metadata.Schema, params.Metadata.Name,
	)
	return r.generic.SampleTableWithQuery(ctx, query, params)
}

// Ping verifies the connection to Oracle database used by this Oracle
// Normally we would just delegate to GenericRepository.Ping, however, that
// implementation executes a 'SELECT 1' query to test for connectivity, and
// Oracle being Oracle does not like this. Instead, we defer to the native
// Ping method implemented by the Oracle DB driver.
func (r *OracleRepository) Ping(ctx context.Context) error {
	return r.generic.GetDb().PingContext(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *OracleRepository) Close() error {
	return r.generic.Close()
}

// OracleConfig is a struct to hold Oracle-specific configuration.
type OracleConfig struct {
	// ServiceName is the Oracle service name.
	ServiceName string
}

// NewOracleConfigFromMap creates a new OracleConfig from the given map. This is
// useful for parsing the Oracle-specific configuration from the
// RepoConfig.Advanced map, for example.
func NewOracleConfigFromMap(cfg map[string]any) (OracleConfig, error) {
	serviceName, err := keyAsString(cfg, configServiceName)
	if err != nil {
		return OracleConfig{}, err
	}
	return OracleConfig{ServiceName: serviceName}, nil
}
