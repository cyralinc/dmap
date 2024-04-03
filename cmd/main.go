package main

import (
	"fmt"

	"github.com/alecthomas/kong"
	log "github.com/sirupsen/logrus"
)

type Globals struct {
	LogLevel     logLevelFlag     `help:"Set the logging level (trace|debug|info|warn|error|fatal)" enum:"trace,debug,info,warn,error,fatal" default:"info"`
	LogFormat    logFormatFlag    `help:"Set the logging format (text|json)" enum:"text,json" default:"text"`
	Version      kong.VersionFlag `name:"version" help:"Print version information and quit"`
	ApiBaseUrl   string           `help:"Base URL of the Dmap API." default:"https://api.dmap.cyral.io"`
	ClientID     string           `help:"API client ID to access the Dmap API."`
	ClientSecret string           `help:"API client secret to access the Dmap API."` //#nosec G101 -- false positive
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
	Globals
	RepoScan RepoScanCmd `cmd:"" help:"Perform data discovery and classification on a data repository."`
}

// version is the application version. It is intended to be set at compile time
// via the linker (e.g. -ldflags="-X main.version=...").
var version string

func main() {
	cli := CLI{
		Globals: Globals{},
	}
	ctx := kong.Parse(
		&cli,
		kong.Name("dmap"),
		kong.Description("Assess your data security posture in AWS."),
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
	err := ctx.Run(&cli.Globals)
	ctx.FatalIfErrorf(err)
}
