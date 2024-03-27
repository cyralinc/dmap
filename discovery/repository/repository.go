package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/semaphore"

	log "github.com/sirupsen/logrus"

	"github.com/cyralinc/dmap/discovery/config"
)

// Repository represents a Dmap data repository, and provides functionality to
// introspect its corresponding schema.
type Repository interface {
	// ListDatabases returns a list of the names of all databases on the server.
	ListDatabases(ctx context.Context) ([]string, error)

	// Introspect will read and analyze the basic properties of the repository
	// and return as a Metadata instance. This includes all the repository's
	// databases, schemas, tables, columns, and attributes.
	Introspect(ctx context.Context) (*Metadata, error)

	// SampleTable samples the table referenced by the TableMetadata meta
	// parameter and returns the sample as a slice of Sample. The parameters for
	// the sample, such as sample size, are passed via the params parameter (see
	// SampleParameters for more details). The returned sample result set
	// contains one Sample for each table row sampled. The length of the results
	// will be less than or equal to the sample size. If there are fewer results
	// than the specified sample size, it is because the table in question had a
	// row count less than the sample size. Prefer small sample sizes to limit
	// impact on the database.
	SampleTable(ctx context.Context, meta *TableMetadata, params SampleParameters) (Sample, error)

	// Ping is meant to be used as a general purpose connectivity test. It
	// should be invoked e.g. in the dry-run mode.
	Ping(ctx context.Context) error

	// Close is meant to be used as a general purpose cleanup. It should be
	// invoked when the Repository is no longer used.
	Close() error
}

// RepoConstructor represents the function signature that all repository
// implementations should use for their constructor functions.
type RepoConstructor func(ctx context.Context, cfg config.RepoConfig) (Repository, error)

var registry = make(map[string]RepoConstructor)

// Register makes a repository available by the provided repository type (repoType).
// If Register is called twice with the same repoType, or if constructor is nil, it
// panics. Note that Register is not thread-safe. It is expected to be called from
// either a package's init function, or from the program's main function.
func Register(repoType string, constructor RepoConstructor) {
	if constructor == nil {
		panic("attempt to register nil constructor for repoType " + repoType)
	}

	if _, dup := registry[repoType]; dup {
		panic("register called twice for repoType " + repoType)
	}

	registry[repoType] = constructor
}

// NewRepository is a factory function to return a concrete Repository implementation
// based on the specified type, e.g. MySQL, PostgreSQL, MSSQL, etc.
func NewRepository(ctx context.Context, cfg config.RepoConfig) (Repository, error) {
	constructor, ok := registry[cfg.Type]
	if !ok {
		return nil, errors.New("unsupported repo type " + cfg.Type)
	}

	repo, err := constructor(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating repository, %w", err)
	}

	return repo, nil
}

// sampleAndErr is a "pair" type intended to be passed to a channel (see
// SampleRepository)
type sampleAndErr struct {
	sample Sample
	err    error
}

// samplesAndErr is a "pair" type intended to be passed to a channel (see
// SampleAllDatabases)
type samplesAndErr struct {
	samples []Sample
	err     error
}

// SampleRepository is a helper function which will sample every table in a
// given repository and return them as a collection of Sample. First the
// repository is introspected by calling Repository.Introspect to return the
// repository metadata (Metadata). Then, for each schema and table in the
// metadata, it calls Repository.SampleTable in a new goroutine. Once all the
// sampling goroutines are finished, their results are collected and returned
// as a slice of Sample.
func SampleRepository(ctx context.Context, repo Repository, params SampleParameters) (
	[]Sample,
	error,
) {
	meta, err := repo.Introspect(ctx)
	if err != nil {
		return nil, fmt.Errorf("error introspecting repository: %w", err)
	}

	// Fan out sample executions
	out := make(chan sampleAndErr)
	numTables := 0
	for _, schemaMeta := range meta.Schemas {
		for _, tableMeta := range schemaMeta.Tables {
			numTables++
			go func(meta *TableMetadata, params SampleParameters) {
				sample, err := repo.SampleTable(ctx, meta, params)
				out <- sampleAndErr{sample: sample, err: err}
			}(tableMeta, params)
		}
	}

	var samples []Sample
	var errs error
	for i := 0; i < numTables; i++ {
		res := <-out
		if res.err != nil {
			errs = multierror.Append(errs, res.err)
		} else {
			samples = append(samples, res.sample)
		}
	}
	close(out)

	if errs != nil {
		return samples, fmt.Errorf("error(s) while sampling repository: %w", errs)
	}

	return samples, nil
}

// SampleAllDatabases uses the given repository to list all the databases on the
// server, and samples each one in parallel by calling SampleRepository for each
// database. The repository is intended to be configured to connect to the
// default database on the server, or at least some database which can be used
// to enumerate the full set of databases on the server. An error will be
// returned if the set of databases cannot be listed. If there is an error
// connecting to or sampling a database, the error will be logged and no samples
// will be returned for that database. Therefore, the returned slice of samples
// contains samples for only the databases which could be discovered and
// successfully sampled, and could potentially be empty if no databases were
// sampled.
func SampleAllDatabases(
	ctx context.Context,
	repo Repository,
	repoCfg config.RepoConfig,
	sampleParams SampleParameters,
) (
	[]Sample,
	error,
) {
	// We assume that this repository will be connected to the default database
	// (or at least some database that can discover all the other databases),
	// and we use that to discover all other databases.
	dbs, err := repo.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing databases: %w", err)
	}

	// Sample each database on a separate goroutine, and send the samples
	// to the 'out' channel. Each slice of samples will be aggregated below
	// on the main goroutine and returned.
	var wg sync.WaitGroup
	out := make(chan samplesAndErr)
	wg.Add(len(dbs))
	// Ensures that we avoid opening more than the specified number of
	// connections.
	var sema *semaphore.Weighted
	if repoCfg.MaxOpenConns > 0 {
		sema = semaphore.NewWeighted(int64(repoCfg.MaxOpenConns))
	}
	for _, db := range dbs {
		go func(db string, cfg config.RepoConfig) {
			defer wg.Done()
			if sema != nil {
				_ = sema.Acquire(ctx, 1)
				defer sema.Release(1)
			}
			cfg.Database = db
			// Create a repository instance for this database. It will be used
			// to connect and sample the database.
			repo, err := NewRepository(ctx, cfg)
			if err != nil {
				log.Errorf("error creating repository instance for database %s: %v", db, err)
				return
			}
			// Close this repository and free up unused resources since we don't
			// need it any longer.
			defer func() { _ = repo.Close() }()
			s, err := SampleRepository(ctx, repo, sampleParams)
			if err != nil && len(s) == 0 {
				log.Errorf("error gathering repository data samples for database %s: %v", db, err)
				return
			}
			// Send the samples for this database to the 'out' channel. The
			// samples for each database will be aggregated into a single slice
			// on the main goroutine and returned.
			out <- samplesAndErr{samples: s, err: err}
		}(db, repoCfg)
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
