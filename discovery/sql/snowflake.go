package sql

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/config"

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

// SnowflakeRepository is a Repository implementation for Snowflake databases.
type SnowflakeRepository struct {
	// The majority of the Repository functionality is delegated to
	// a generic SQL repository instance (genericSqlRepo).
	genericSqlRepo *GenericRepository
}

// SnowflakeRepository implements Repository
var _ Repository = (*SnowflakeRepository)(nil)

// NewSnowflakeRepository creates a new SnowflakeRepository.
func NewSnowflakeRepository(cfg config.RepoConfig) (*SnowflakeRepository, error) {
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

// TODO: godoc -ccampo 2024-04-02
func (r *SnowflakeRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.genericSqlRepo.ListDatabasesWithQuery(ctx, SnowflakeDatabaseQuery)
}

// TODO: godoc -ccampo 2024-04-02
func (r *SnowflakeRepository) Introspect(ctx context.Context) (*Metadata, error) {
	return r.genericSqlRepo.Introspect(ctx)
}

// TODO: godoc -ccampo 2024-04-02
func (r *SnowflakeRepository) SampleTable(
	ctx context.Context,
	meta *TableMetadata,
	params SampleParameters,
) (Sample, error) {
	return r.genericSqlRepo.SampleTable(ctx, meta, params)
}

// TODO: godoc -ccampo 2024-04-02
func (r *SnowflakeRepository) Ping(ctx context.Context) error {
	return r.genericSqlRepo.Ping(ctx)
}

// TODO: godoc -ccampo 2024-04-02
func (r *SnowflakeRepository) Close() error {
	return r.genericSqlRepo.Close()
}

// TODO: godoc -ccampo 2024-04-02
type SnowflakeConfig struct {
	Account   string
	Role      string
	Warehouse string
}

// ParseSnowflakeConfig produces a config structure with Snowflake-specific
// parameters found in the repo config.
func ParseSnowflakeConfig(cfg config.RepoConfig) (*SnowflakeConfig, error) {
	snowflakeCfg, err := config.FetchAdvancedConfigString(
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
