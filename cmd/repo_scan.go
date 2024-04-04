package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/gobwas/glob"

	"github.com/cyralinc/dmap/sql"
)

type RepoScanCmd struct {
	Type         string         `help:"Type of repository to connect to (postgres|mysql|oracle|sqlserver|snowflake|redshift|denodo)." enum:"postgres,mysql,oracle,sqlserver,snowflake,redshift,denodo" required:""`
	ExternalID   string         `help:"External ID of the repository." required:""`
	Host         string         `help:"Hostname of the repository." required:""`
	Port         uint16         `help:"Port of the repository." required:""`
	User         string         `help:"Username to connect to the sql." required:""`
	Password     string         `help:"Password to connect to the sql." required:""`
	Database     string         `help:"Name of the database to connect to. If not specified, the default database is used (if possible)."`
	Advanced     map[string]any `help:"Advanced configuration for the sql."`
	IncludePaths GlobFlag       `help:"List of glob patterns to include when introspecting the database(s)." default:"*"`
	ExcludePaths GlobFlag       `help:"List of glob patterns to exclude when introspecting the database(s)." default:""`
	MaxOpenConns uint           `help:"Maximum number of open connections to the database." default:"10"`
	SampleSize   uint           `help:"Number of rows to sample from the repository (per table)." default:"5"`
	Offset       uint           `help:"Offset to start sampling from." default:"0"`
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

func (cmd *RepoScanCmd) Run(_ *Globals) error {
	ctx := context.Background()
	// Configure and instantiate the scanner.
	cfg := sql.ScannerConfig{
		RepoType: cmd.Type,
		RepoConfig: sql.RepoConfig{
			Host:         cmd.Host,
			Port:         cmd.Port,
			User:         cmd.User,
			Password:     cmd.Password,
			Database:     cmd.Database,
			MaxOpenConns: cmd.MaxOpenConns,
			Advanced:     cmd.Advanced,
		},
		IncludePaths: cmd.IncludePaths,
		ExcludePaths: cmd.ExcludePaths,
		SampleSize:   cmd.SampleSize,
		Offset:       cmd.Offset,
	}
	scanner, err := sql.NewScanner(cfg)
	if err != nil {
		return fmt.Errorf("error creating new scanner: %w", err)
	}
	// Scan the repository.
	results, err := scanner.Scan(ctx)
	if err != nil {
		return fmt.Errorf("error scanning repository: %w", err)
	}
	// Print the results to stdout.
	jsonResults, err := json.MarshalIndent(results, "", "    ")
	if err != nil {
		return fmt.Errorf("error marshalling results: %w", err)
	}
	fmt.Println(string(jsonResults))
	// TODO: publish results to the API -ccampo 2024-04-03
	return nil
}
