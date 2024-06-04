package main

import (
	"fmt"

	"github.com/alecthomas/kong"
	log "github.com/sirupsen/logrus"
)

type Globals struct {
	ClientID     string           `help:"API client ID to access the Dmap API. Optional, but required to publish results to the Dmap service."`
	ClientSecret string           `help:"API client secret to access the Dmap API. Optional, but required to publish results to the Dmap service."` //#nosec G101 -- false positive
	ApiBaseUrl   string           `help:"Base URL of the Dmap API." default:"https://api.dmap.cyral.io"`
	LogLevel     logLevelFlag     `help:"Set the logging level (trace|debug|info|warn|error|fatal)" enum:"trace,debug,info,warn,error,fatal" default:"info"`
	LogFormat    logFormatFlag    `help:"Set the logging format (text|json)" enum:"text,json" default:"text"`
	Version      kong.VersionFlag `name:"version" help:"Print version information and quit"`
}

type logLevelFlag string

func (l logLevelFlag) AfterApply() error {
	lvl, err := log.ParseLevel(string(l))
	if err != nil {
		return err
	}
	log.SetLevel(lvl)
	return nil
}

type logFormatFlag string

func (l logFormatFlag) AfterApply() error {
	switch l {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
		return nil
	case "text":
		log.SetFormatter(
			&log.TextFormatter{
				FullTimestamp:          true,
				DisableLevelTruncation: true,
				PadLevelText:           true,
			},
		)
		return nil
	default:
		return fmt.Errorf("unsupported log format: %s", string(l))
	}
}

type CLI struct {
	*Globals
	RepoScan RepoScanCmd `cmd:"" help:"Perform data discovery and classification on a data repository."`
}

var (
	// version is the application version. It is intended to be set at compile
	// time via the linker (e.g. -ldflags="-X main.version=...").
	version = "dev"
	// globals holds the application's global flags. It is a package-level
	// variable to allow access to the flags from any part of the application,
	// such as validation functions.
	globals Globals
)

func main() {
	cli := CLI{Globals: &globals}
	ctx := kong.Parse(
		&cli,
		kong.Name("dmap"),
		kong.Description("Discover your data repositories and classify their sensitive data."),
		kong.UsageOnError(),
		kong.ConfigureHelp(
			kong.HelpOptions{
				Compact: true,
			},
		),
		kong.Vars{
			"version": version,
		},
	)
	err := ctx.Run(cli.Globals)
	ctx.FatalIfErrorf(err)
}
