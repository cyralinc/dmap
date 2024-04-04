package sql

import (
	"context"
	"fmt"

	// Snowflake DB driver
	_ "github.com/snowflakedb/gosnowflake"
)

const (
	RepoTypeSnowflake      = "snowflake"
	snowflakeDatabaseQuery = `
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

// SnowflakeRepository is a Repository implementation for Snowflake databases.
type SnowflakeRepository struct {
	// The majority of the Repository functionality is delegated to
	// a generic SQL repository instance.
	generic *GenericRepository
}

// SnowflakeRepository implements Repository
var _ Repository = (*SnowflakeRepository)(nil)

// NewSnowflakeRepository creates a new SnowflakeRepository.
func NewSnowflakeRepository(cfg RepoConfig) (*SnowflakeRepository, error) {
	snowflakeCfg, err := NewSnowflakeConfigFromMap(cfg.Advanced)
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
	generic, err := NewGenericRepository(RepoTypeSnowflake, database, connStr, cfg.MaxOpenConns)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &SnowflakeRepository{generic: generic}, nil
}

// ListDatabases returns a list of the names of all databases on the server by
// using a Snowflake-specific database query. It delegates the actual work to
// GenericRepository.ListDatabasesWithQuery - see that method for more details.
func (r *SnowflakeRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.generic.ListDatabasesWithQuery(ctx, snowflakeDatabaseQuery)
}

// Introspect delegates introspection to GenericRepository. See
// Repository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *SnowflakeRepository) Introspect(ctx context.Context, params IntrospectParameters) (*Metadata, error) {
	return r.generic.Introspect(ctx, params)
}

// SampleTable delegates sampling to GenericRepository. See
// Repository.SampleTable and GenericRepository.SampleTable for more details.
func (r *SnowflakeRepository) SampleTable(
	ctx context.Context,
	params SampleParameters,
) (Sample, error) {
	return r.generic.SampleTable(ctx, params)
}

// Ping delegates the ping to GenericRepository. See Repository.Ping and
// GenericRepository.Ping for more details.
func (r *SnowflakeRepository) Ping(ctx context.Context) error {
	return r.generic.Ping(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *SnowflakeRepository) Close() error {
	return r.generic.Close()
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

// NewSnowflakeConfigFromMap creates a new SnowflakeConfig from the given map.
// This is useful for parsing the Snowflake-specific configuration from the
// RepoConfig.Advanced map, for example.
func NewSnowflakeConfigFromMap(cfg map[string]any) (SnowflakeConfig, error) {
	acct, err := keyAsString(cfg, configAccount)
	if err != nil {
		return SnowflakeConfig{}, err
	}
	role, err := keyAsString(cfg, configRole)
	if err != nil {
		return SnowflakeConfig{}, err
	}
	warehouse, err := keyAsString(cfg, configWarehouse)
	if err != nil {
		return SnowflakeConfig{}, err
	}
	return SnowflakeConfig{
		Account:   acct,
		Role:      role,
		Warehouse: warehouse,
	}, nil
}
