package oracle

import (
	"github.com/cyralinc/dmap/discovery/config"
)

const (
	configServiceName = "service-name"
)

type Config struct {
	ServiceName string
}

func ParseConfig(cfg config.RepoConfig) (*Config, error) {
	oracleCfg, err := config.FetchAdvancedConfigString(
		cfg,
		RepoTypeOracle,
		[]string{configServiceName},
	)
	if err != nil {
		return nil, err
	}
	return &Config{
		ServiceName: oracleCfg[configServiceName],
	}, nil
}
