package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/gobwas/glob"

	"github.com/cyralinc/dmap/internal/api"
	"github.com/cyralinc/dmap/sql"
)

type RepoScanCmd struct {
	Type           string         `help:"Type of repository to connect to (postgres|mysql|oracle|sqlserver|snowflake|redshift|denodo)." enum:"postgres,mysql,oracle,sqlserver,snowflake,redshift,denodo" required:""`
	Host           string         `help:"Hostname of the repository." required:""`
	Port           uint16         `help:"Port of the repository." required:""`
	User           string         `help:"Username to connect to the repository." required:""`
	Password       string         `help:"Password to connect to the repository." required:""`
	RepoID         string         `help:"The ID of the repository used by the Dmap service to identify the data repository. For RDS or Redshift, this is the ARN of the database. Optional, but required to publish the scan results Dmap service."`
	Database       string         `help:"Name of the database to connect to. If not specified, the default database is used (if possible)."`
	Advanced       map[string]any `help:"Advanced configuration for the repository, semicolon separated (e.g. key1=value1;key2=value2). Please see the documentation for details on how to provide this argument for specific repository types."`
	IncludePaths   GlobFlag       `help:"List of glob patterns to include when introspecting the database(s), semicolon separated (e.g. foo*;bar*;*.baz)." default:"*"`
	ExcludePaths   GlobFlag       `help:"List of glob patterns to exclude when introspecting the database(s), semicolon separated (e.g. foo*;bar*;*.baz)."`
	MaxOpenConns   uint           `help:"Maximum number of open connections to the database." default:"10"`
	MaxParallelDbs uint           `help:"Maximum number of parallel databases scanned at once. If zero, there is no limit." default:"0"`
	MaxConcurrency uint           `help:"Maximum number of concurrent query goroutines. If zero, there is no limit." default:"0"`
	QueryTimeout   time.Duration  `help:"Maximum time a query can run before being cancelled. If zero, there is no timeout." default:"0s"`
	SampleSize     uint           `help:"Number of rows to sample from the repository (per table)." default:"5"`
	Offset         uint           `help:"Offset to start sampling each table from." default:"0"`
	LabelYamlFile  string         `help:"Filename of the yaml file containing the custom set of data labels (e.g. /path/to/labels.yaml). If omitted, a set of predefined labels is used."`
	Silent         bool           `help:"Do not print the results to stdout." short:"s"`
}

func (cmd *RepoScanCmd) Validate() error {
	if cmd.RepoID != "" {
		if globals.ClientID == "" || globals.ClientSecret == "" {
			return fmt.Errorf("repo-id was provided, but client-id and client-secret are also required to publish results to Dmap")
		}
	}
	return nil
}

// GlobFlag is a kong.MapperValue implementation that represents a glob pattern.
type GlobFlag []glob.Glob

// Decode parses the glob patterns and compiles them into glob.Glob objects. It
// is an implementation of kong.MapperValue's Decode method.
func (g GlobFlag) Decode(ctx *kong.DecodeContext) error {
	var patterns string
	if err := ctx.Scan.PopValueInto("patterns", &patterns); err != nil {
		return err
	}
	var parsedPatterns []glob.Glob
	for _, pattern := range strings.Split(patterns, ";") {
		parsedPattern, err := glob.Compile(pattern)
		if err != nil {
			return fmt.Errorf("cannot compile %s pattern: %w", pattern, err)
		}
		parsedPatterns = append(parsedPatterns, parsedPattern)
	}
	ctx.Value.Target.Set(reflect.ValueOf(GlobFlag(parsedPatterns)))
	return nil
}

func (cmd *RepoScanCmd) Run(globals *Globals) error {
	ctx := context.Background()
	// Configure and instantiate the scanner.
	cfg := sql.ScannerConfig{
		RepoType: cmd.Type,
		RepoConfig: sql.RepoConfig{
			Host:           cmd.Host,
			Port:           cmd.Port,
			User:           cmd.User,
			Password:       cmd.Password,
			Database:       cmd.Database,
			MaxOpenConns:   cmd.MaxOpenConns,
			MaxParallelDbs: cmd.MaxParallelDbs,
			MaxConcurrency: cmd.MaxConcurrency,
			QueryTimeout:   cmd.QueryTimeout,
			Advanced:       cmd.Advanced,
		},
		IncludePaths:       cmd.IncludePaths,
		ExcludePaths:       cmd.ExcludePaths,
		SampleSize:         cmd.SampleSize,
		Offset:             cmd.Offset,
		LabelsYamlFilename: cmd.LabelYamlFile,
	}
	scanner, err := sql.NewScanner(ctx, cfg)
	if err != nil {
		return fmt.Errorf("error creating new scanner: %w", err)
	}
	// Scan the repository.
	results, err := scanner.Scan(ctx)
	if err != nil {
		return fmt.Errorf("error scanning repository: %w", err)
	}
	if !cmd.Silent {
		// Print the results to stdout.
		jsonResults, err := json.MarshalIndent(results, "", "    ")
		if err != nil {
			return fmt.Errorf("error marshalling results: %w", err)
		}
		fmt.Println(string(jsonResults))
	}
	// Publish the results to the Dmap API.
	if cmd.RepoID != "" {
		client := api.NewDmapClient(globals.ApiBaseUrl, globals.ClientID, globals.ClientSecret)
		agent := "dmap-cli_" + version
		if err := client.PublishRepoScanResults(ctx, agent, cmd.RepoID, results); err != nil {
			return fmt.Errorf("error publishing results to Dmap API: %w", err)
		}
	}
	return nil
}
