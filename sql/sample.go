package sql

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

// sampleAndErr is a "pair" type intended to be passed to a channel (see
// sampleDb)
type sampleAndErr struct {
	sample Sample
	err    error
}

// samplesAndErr is a "pair" type intended to be passed to a channel (see
// sampleAllDbs)
type samplesAndErr struct {
	samples []Sample
	err     error
}

// sampleAllDbs uses the given Repository to list all the
// databases on the server, and samples each one in parallel by calling
// sampleDb for each database. The repository is intended to be
// configured to connect to the default database on the server, or at least some
// database which can be used to enumerate the full set of databases on the
// server. An error will be returned if the set of databases cannot be listed.
// If there is an error connecting to or sampling a database, the error will be
// logged and no samples will be returned for that database. Therefore, the
// returned slice of samples contains samples for only the databases which could
// be discovered and successfully sampled, and could potentially be empty if no
// databases were sampled.
func sampleAllDbs(
	ctx context.Context,
	ctor RepoConstructor,
	cfg RepoConfig,
	introspectParams IntrospectParameters,
	sampleSize, offset uint,
) (
	[]Sample,
	error,
) {
	// Create a repository instance that will be used to list all the databases
	// on the server.
	repo, err := ctor(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating repository instance: %w", err)
	}
	defer func() { _ = repo.Close() }()

	// We assume that this repository will be connected to the default database
	// (or at least some database that can discover all the other databases),
	// and we use that to discover all other databases.
	dbs, err := repo.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing databases: %w", err)
	}

	// Sample each database on a separate goroutine, and send the samples to
	// the 'out' channel. Each slice of samples will be aggregated below on the
	// main goroutine and returned.
	var wg sync.WaitGroup
	out := make(chan samplesAndErr)
	wg.Add(len(dbs))
	// Ensures that we avoid opening more than the specified number of
	// connections.
	var sema *semaphore.Weighted
	if cfg.MaxOpenConns > 0 {
		sema = semaphore.NewWeighted(int64(cfg.MaxOpenConns))
	}
	for _, db := range dbs {
		go func(db string, cfg RepoConfig) {
			defer wg.Done()
			if sema != nil {
				_ = sema.Acquire(ctx, 1)
				defer sema.Release(1)
			}
			// Create a repository instance for this specific database. It will
			// be used to connect to and sample the database.
			cfg.Database = db
			repo, err := ctor(ctx, cfg)
			if err != nil {
				log.WithError(err).Errorf("error creating repository instance for database %s", db)
				return
			}
			defer func() { _ = repo.Close() }()
			// Sample the database.
			s, err := sampleDb(ctx, repo, introspectParams, sampleSize, offset)
			if err != nil && len(s) == 0 {
				log.WithError(err).Errorf("error gathering repository data samples for database %s", db)
				return
			}
			// Send the samples for this database to the 'out' channel. The
			// samples for each database will be aggregated into a single slice
			// on the main goroutine and returned.
			out <- samplesAndErr{samples: s, err: err}
		}(db, cfg)
	}

	// Start a goroutine to close the 'out' channel once all the goroutines
	// we launched above are done. This will allow the aggregation range loop
	// below to terminate properly. Note that this must start after the wg.Add
	// call. See https://go.dev/blog/pipelines ("Fan-out, fan-in" section).
	go func() {
		wg.Wait()
		close(out)
	}()

	// Aggregate and return the results.
	var ret []Sample
	var errs error
	for res := range out {
		ret = append(ret, res.samples...)
		if res.err != nil {
			errs = multierror.Append(errs, res.err)
		}
	}
	return ret, errs
}

// sampleDb is a helper function which will sample every table in a
// given repository and return them as a collection of Sample. First the
// repository is introspected by calling Introspect to return the
// repository metadata (Metadata). Then, for each schema and table in the
// metadata, it calls SampleTable in a new goroutine. Once all the
// sampling goroutines are finished, their results are collected and returned
// as a slice of Sample.
func sampleDb(
	ctx context.Context,
	repo Repository,
	introspectParams IntrospectParameters,
	sampleSize, offset uint,
) (
	[]Sample,
	error,
) {
	// Introspect the repository to get the metadata.
	meta, err := repo.Introspect(ctx, introspectParams)
	if err != nil {
		return nil, fmt.Errorf("error introspecting repository: %w", err)
	}

	// Fan out sample executions.
	out := make(chan sampleAndErr)
	numTables := 0
	for _, schemaMeta := range meta.Schemas {
		for _, tableMeta := range schemaMeta.Tables {
			numTables++
			go func(meta *TableMetadata) {
				params := SampleParameters{
					Metadata:   meta,
					SampleSize: sampleSize,
					Offset:     offset,
				}
				sample, err := repo.SampleTable(ctx, params)
				select {
				case <-ctx.Done():
					return
				case out <- sampleAndErr{sample: sample, err: err}:
				}
			}(tableMeta)
		}
	}

	// Aggregate and return the results.
	var samples []Sample
	var errs error
	for i := 0; i < numTables; i++ {
		select {
		case <-ctx.Done():
			return samples, ctx.Err()
		case res:= <-out:
			if res.err != nil {
				errs = multierror.Append(errs, res.err)
			} else {
				samples = append(samples, res.sample)
			}
		}
	}
	close(out)
	if errs != nil {
		return samples, fmt.Errorf("error(s) while sampling repository: %w", errs)
	}
	return samples, nil
}
