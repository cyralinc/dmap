package snowflake

import (
	"github.com/cyralinc/dmap/discovery/config"
)

const (
	configAccount   = "account"
	configRole      = "role"
	configWarehouse = "warehouse"
)

type Config struct {
	Account   string
	Role      string
	Warehouse string
}

// ParseConfig produces a config structure with Snowflake-specific parameters
// found in the repo config.
func ParseConfig(cfg config.RepoConfig) (*Config, error) {
	snowflakeCfg, err := config.FetchAdvancedConfigString(
		cfg,
		RepoTypeSnowflake,
		[]string{configAccount, configRole, configWarehouse},
	)
	if err != nil {
		return nil, err
	}
	return &Config{
		Account:   snowflakeCfg[configAccount],
		Role:      snowflakeCfg[configRole],
		Warehouse: snowflakeCfg[configWarehouse],
	}, nil

}
