package main

import (
	"context"

	"github.com/cyralinc/dmap/discovery"
	"github.com/cyralinc/dmap/discovery/config"
)

type RepoScanCmd struct {
	config.Config
}

func (cmd *RepoScanCmd) Run(_ *Globals) error {
	scanner := discovery.NewScanner(&cmd.Config)
	defer scanner.Cleanup()
	return scanner.InitAndRun(context.Background())
}
