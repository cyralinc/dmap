package discovery

import (
	"context"
	"fmt"

	// Snowflake DB driver
	_ "github.com/snowflakedb/gosnowflake"
)

const (
	RepoTypeSnowflake      = "snowflake"
	SnowflakeDatabaseQuery = `
SELECT 
    DATABASE_NAME 
FROM 
    INFORMATION_SCHEMA.DATABASES 
WHERE 
    IS_TRANSIENT = 'NO'
`
	configAccount   = "account"
	configRole      = "role"
	configWarehouse = "warehouse"
)

// SnowflakeRepository is a SQLRepository implementation for Snowflake databases.
type SnowflakeRepository struct {
	// The majority of the SQLRepository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *GenericRepository
}

// SnowflakeRepository implements SQLRepository
var _ SQLRepository = (*SnowflakeRepository)(nil)

// NewSnowflakeRepository creates a new SnowflakeRepository.
func NewSnowflakeRepository(cfg RepoConfig) (*SnowflakeRepository, error) {
	snowflakeCfg, err := ParseSnowflakeConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("error parsing snowflake config: %w", err)
	}
	database := cfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "SNOWFLAKE"
	}
	connStr := fmt.Sprintf(
		"%s:%s@%s/%s?role=%s&warehouse=%s",
		cfg.User,
		cfg.Password,
		snowflakeCfg.Account,
		cfg.Database,
		snowflakeCfg.Role,
		snowflakeCfg.Warehouse,
	)
	sqlRepo, err := NewGenericRepository(
		cfg.Host,
		RepoTypeSnowflake,
		database,
		connStr,
		cfg.MaxOpenConns,
		cfg.IncludePaths,
		cfg.ExcludePaths,
	)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &SnowflakeRepository{genericSqlRepo: sqlRepo}, nil
}

// ListDatabases returns a list of the names of all databases on the server by
// using a Snowflake-specific database query. It delegates the actual work to
// GenericRepository.ListDatabasesWithQuery - see that method for more details.
func (r *SnowflakeRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.genericSqlRepo.ListDatabasesWithQuery(ctx, SnowflakeDatabaseQuery)
}

// Introspect delegates introspection to GenericRepository. See
// SQLRepository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *SnowflakeRepository) Introspect(ctx context.Context) (*Metadata, error) {
	return r.genericSqlRepo.Introspect(ctx)
}

// SampleTable delegates sampling to GenericRepository. See
// SQLRepository.SampleTable and GenericRepository.SampleTable for more details.
func (r *SnowflakeRepository) SampleTable(
	ctx context.Context,
	meta *TableMetadata,
	params SampleParameters,
) (Sample, error) {
	return r.genericSqlRepo.SampleTable(ctx, meta, params)
}

// Ping delegates the ping to GenericRepository. See SQLRepository.Ping and
// GenericRepository.Ping for more details.
func (r *SnowflakeRepository) Ping(ctx context.Context) error {
	return r.genericSqlRepo.Ping(ctx)
}

// Close delegates the close to GenericRepository. See SQLRepository.Close and
// GenericRepository.Close for more details.
func (r *SnowflakeRepository) Close() error {
	return r.genericSqlRepo.Close()
}

// SnowflakeConfig holds Snowflake-specific configuration parameters.
type SnowflakeConfig struct {
	// Account is the Snowflake account name.
	Account string
	// Role is the Snowflake role name.
	Role string
	// Warehouse is the Snowflake warehouse name.
	Warehouse string
}

// ParseSnowflakeConfig produces a config structure with Snowflake-specific
// parameters found in the repo  The Snowflake account, role, and
// warehouse are required in the advanced
func ParseSnowflakeConfig(cfg RepoConfig) (*SnowflakeConfig, error) {
	snowflakeCfg, err := FetchAdvancedConfigString(
		cfg,
		RepoTypeSnowflake,
		[]string{configAccount, configRole, configWarehouse},
	)
	if err != nil {
		return nil, fmt.Errorf("error fetching advanced config string: %w", err)
	}
	return &SnowflakeConfig{
		Account:   snowflakeCfg[configAccount],
		Role:      snowflakeCfg[configRole],
		Warehouse: snowflakeCfg[configWarehouse],
	}, nil
}
