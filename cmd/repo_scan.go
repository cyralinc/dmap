package main

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/scan"
)

type RepoScanCmd struct {
	scan.RepoScannerConfig
}

func (cmd *RepoScanCmd) Run(_ *Globals) error {
	ctx := context.Background()
	scanner, err := scan.NewRepoScanner(ctx, cmd.RepoScannerConfig)
	if err != nil {
		return fmt.Errorf("error creating new scanner: %w", err)
	}
	defer scanner.Cleanup()
	return scanner.Scan(ctx)
}
