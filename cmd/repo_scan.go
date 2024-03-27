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
	craw := discovery.NewScanner(&cmd.Config)
	defer craw.Cleanup()
	return craw.InitAndRun(context.Background())
}
