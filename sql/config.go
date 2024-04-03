package sql

import (
	"fmt"
)

const configConnOpts = "connection-string-args"

// RepoConfig is the necessary configuration to connect to a data sql.
type RepoConfig struct {
	// Host is the hostname of the database.
	Host string
	// Port is the port of the database.
	Port uint16
	// User is the username to connect to the database.
	User string
	// Password is the password to connect to the database.
	Password string
	// Database is the name of the database to connect to.
	Database string
	// MaxOpenConns is the maximum number of open connections to the database.
	MaxOpenConns uint
	// Advanced is a map of advanced configuration options.
	Advanced map[string]any
}

// FetchAdvancedConfigString fetches a map in the repo advanced configuration,
// for a given repo and set of parameters. Example:
//
// repo-advanced:
//
//	snowflake:
//	  account: exampleAccount
//	  role: exampleRole
//	  warehouse: exampleWarehouse
//
// Calling FetchAdvancedMapConfig(<the repo config above>, "snowflake",
// []string{"account", "role", "warehouse"}) returns the map
//
// {"account": "exampleAccount", "role": "exampleRole", "warehouse":
// "exampleWarehouse"}
//
// The suffix 'String' means that the values of the map are strings. This gives
// room to have FetchAdvancedConfigList or FetchAdvancedConfigMap, for example,
// without name conflicts.
func FetchAdvancedConfigString(
	cfg RepoConfig,
	repo string,
	parameters []string,
) (map[string]string, error) {
	advancedCfg, err := getAdvancedConfig(cfg, repo)
	if err != nil {
		return nil, err
	}
	repoSpecificMap := make(map[string]string)
	for _, key := range parameters {
		var valInterface any
		var val string
		var ok bool
		if valInterface, ok = advancedCfg[key]; !ok {
			return nil, fmt.Errorf("unable to find '%s' in %s advanced config", key, repo)
		}
		if val, ok = valInterface.(string); !ok {
			return nil, fmt.Errorf("'%s' in %s config must be a string", key, repo)
		}
		repoSpecificMap[key] = val
	}
	return repoSpecificMap, nil
}

// getAdvancedConfig gets the Advanced field in a repo config and converts it to
// a map[string]any. In every step, it checks for error and generates
// nice messages.
func getAdvancedConfig(cfg RepoConfig, repo string) (map[string]any, error) {
	advancedCfgInterface, ok := cfg.Advanced[repo]
	if !ok {
		return nil, fmt.Errorf("unable to find '%s' in advanced config", repo)
	}
	advancedCfg, ok := advancedCfgInterface.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("'%s' in advanced config is not a map", repo)
	}
	return advancedCfg, nil
}
