package postgresql

import (
	"github.com/cyralinc/dmap/discovery/config"
)

type Config struct {
	ConnOptsStr string
}

// ParseConfig produces a config structure with PostgreSQL-specific parameters
// found in the repo config.
func ParseConfig(cfg config.RepoConfig) (*Config, error) {
	connOptsStr, err := config.BuildConnOptsStr(cfg)
	if err != nil {
		return nil, err
	}

	return &Config{
		ConnOptsStr: connOptsStr,
	}, nil
}
