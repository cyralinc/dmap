package config

import (
	"fmt"
	"strings"
)

const (
	configConnOpts = "connection-string-args"
)

// BuildConnOptsStr parses the repo config to produce a string in the format
// "?option=value&option2=value2". Example:
//
//	BuildConnOptsStr(config.RepoConfig{
//	    Advanced: map[string]any{
//	        "connection-string-args": []any{"sslmode=disable"},
//	    },
//	})
//
// returns ("?sslmode=disable", nil).
func BuildConnOptsStr(cfg RepoConfig) (string, error) {
	connOptsMap, err := MapFromConnOpts(cfg)
	if err != nil {
		return "", fmt.Errorf("connection options: %w", err)
	}

	connOptsStr := ""
	for key, val := range connOptsMap {
		// Don't add if the value is empty, since that would make the
		// string malformed.
		if val != "" {
			if connOptsStr == "" {
				connOptsStr += fmt.Sprintf("%s=%s", key, val)
			} else {
				// Need & for subsequent options
				connOptsStr += fmt.Sprintf("&%s=%s", key, val)
			}
		}
	}
	// Only add ? if connection string is not empty
	if connOptsStr != "" {
		connOptsStr = "?" + connOptsStr
	}

	return connOptsStr, nil
}

// MapFromConnOpts builds a map from the list of connection options given in the
// config. Each option has the format 'option=value'. Err only if the config is
// malformed, to inform user.
func MapFromConnOpts(cfg RepoConfig) (map[string]string, error) {
	m := make(map[string]string)
	connOptsInterface, ok := cfg.Advanced[configConnOpts]
	if !ok {
		return nil, nil
	}
	connOpts, ok := connOptsInterface.([]any)
	if !ok {
		return nil, fmt.Errorf("'%s' is not a list", configConnOpts)
	}
	for _, optInterface := range connOpts {
		opt, ok := optInterface.(string)
		if !ok {
			return nil, fmt.Errorf("'%v' is not a string", optInterface)
		}
		splitOpt := strings.Split(opt, "=")
		if len(splitOpt) != 2 {
			return nil, fmt.Errorf(
				"malformed '%s'. "+
					"Please follow the format 'option=value'", configConnOpts,
			)
		}
		key := splitOpt[0]
		val := splitOpt[1]
		m[key] = val
	}
	return m, nil
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
