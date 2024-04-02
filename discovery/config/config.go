package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/gobwas/glob"
)

const configConnOpts = "connection-string-args"

// Config is the configuration for the application.
type Config struct {
	Dmap DmapConfig `embed:""`
	Repo RepoConfig `embed:""`
}

// DmapConfig is the necessary configuration to connect to the Dmap API.
type DmapConfig struct {
	ApiBaseUrl   string `help:"Base URL of the Dmap API." default:"https://api.dmap.cyral.io"`
	ClientID     string `help:"API client ID to access the Dmap API."`
	ClientSecret string `help:"API client secret to access the Dmap API."` //#nosec G101 -- false positive
}

// RepoConfig is the necessary configuration to connect to a data sql.
type RepoConfig struct {
	Type         string         `help:"Type of repository to connect to (postgres|mysql|oracle|sqlserver|snowflake|redshift|denodo)." enum:"postgres,mysql,oracle,sqlserver,snowflake,redshift,denodo" required:""`
	Host         string         `help:"Hostname of the sql." required:""`
	Port         uint16         `help:"Port of the sql." required:""`
	User         string         `help:"Username to connect to the sql." required:""`
	Password     string         `help:"Password to connect to the sql." required:""`
	Advanced     map[string]any `help:"Advanced configuration for the sql."`
	Database     string         `help:"Name of the database to connect to. If not specified, the default database is used."`
	MaxOpenConns uint           `help:"Maximum number of open connections to the sql." default:"10"`
	SampleSize   uint           `help:"Number of rows to sample from the repository (per table)." default:"5"`
	IncludePaths GlobFlag       `help:"List of glob patterns to include when querying the database(s), as a comma separated list." default:"*"`
	ExcludePaths GlobFlag       `help:"List of glob patterns to exclude when querying the database(s), as a comma separated list." default:""`
}

// GlobFlag is a kong.MapperValue implementation that represents a glob pattern.
type GlobFlag []glob.Glob

// Decode parses the glob patterns and compiles them into glob.Glob objects. It
// is an implementation of kong.MapperValue's Decode method.
func (g GlobFlag) Decode(ctx *kong.DecodeContext) error {
	var patterns string
	if err := ctx.Scan.PopValueInto("string", &patterns); err != nil {
		return err
	}
	var parsedPatterns []glob.Glob
	for _, pattern := range strings.Split(patterns, ",") {
		parsedPattern, err := glob.Compile(pattern)
		if err != nil {
			return fmt.Errorf("cannot compile %s pattern: %w", pattern, err)
		}
		parsedPatterns = append(parsedPatterns, parsedPattern)
	}
	ctx.Value.Target.Set(reflect.ValueOf(GlobFlag(parsedPatterns)))
	return nil
}

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
	connOptsMap, err := mapFromConnOpts(cfg)
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

// mapFromConnOpts builds a map from the list of connection options given in the
// config. Each option has the format 'option=value'. Err only if the config is
// malformed, to inform user.
func mapFromConnOpts(cfg RepoConfig) (map[string]string, error) {
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
