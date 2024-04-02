package main

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery"
	"github.com/cyralinc/dmap/discovery/config"
)

type RepoScanCmd struct {
	config.Config
}

func (cmd *RepoScanCmd) Run(_ *Globals) error {
	ctx := context.Background()
	scanner, err := discovery.NewScanner(ctx, &cmd.Config)
	if err != nil {
		return fmt.Errorf("error creating new scanner: %w", err)
	}
	defer scanner.Cleanup()
	return scanner.Scan(ctx)
}
