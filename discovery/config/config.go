package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/gobwas/glob"
)

// TODO: godoc -ccampo 2024-03-27
type Config struct {
	Repo RepoConfig `embed:""`
	Dmap DmapConfig `embed:""`
}

// TODO: godoc -ccampo 2024-03-27
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

// TODO: godoc -ccampo 2024-03-27
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

// TODO: godoc -ccampo 2024-03-27
type DmapConfig struct {
	ApiBaseUrl   string `help:"Base URL of the Dmap API." default:"https://api.dmap.cyral.io"`
	ClientID     string `help:"API client ID to access the Dmap API."`
	ClientSecret string `help:"API client secret to access the Dmap API."` //#nosec G101 -- false positive
}
