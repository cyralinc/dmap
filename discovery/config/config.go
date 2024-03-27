package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/gobwas/glob"
)

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

// RepoConfig is the necessary configuration to connect to a data repository.
type RepoConfig struct {
	Type         string         `help:"Type of repository to connect to (postgres|mysql|oracle|sqlserver|snowflake|redshift|denodo)." enum:"postgres,mysql,oracle,sqlserver,snowflake,redshift,denodo" required:""`
	Host         string         `help:"Hostname of the repository." required:""`
	Port         uint16         `help:"Port of the repository." required:""`
	User         string         `help:"Username to connect to the repository." required:""`
	Password     string         `help:"Password to connect to the repository." required:""`
	Advanced     map[string]any `help:"Advanced configuration for the repository."`
	Database     string         `help:"Name of the database to connect to. If not specified, the default database is used."`
	MaxOpenConns uint           `help:"Maximum number of open connections to the repository." default:"10"`
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
