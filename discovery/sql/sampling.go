package sql

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	"github.com/cyralinc/dmap/discovery/config"
)

// SampleParameters contains all parameters necessary to sample a table.
type SampleParameters struct {
	SampleSize uint
	Offset     uint
}

// Sample represents a sample of a database table. The Metadata field contains
// metadata about the sample itself. The actual results of the sample, which
// are represented by a set of database rows, are contained in the Results
// field.
type Sample struct {
	Metadata SampleMetadata
	Results  []SampleResult
}

// SampleMetadata contains the metadata associated with a given sample, such as
// repo name, database name, table, schema, and query (if applicable). This can
// be used for diagnostic and informational purposes when analyzing a
// particular sample.
type SampleMetadata struct {
	Repo     string
	Database string
	Schema   string
	Table    string
}

// SampleResult stores the results from a single database sample. It is
// equivalent to a database row, where the map key is the column name and the
// map value is the column value.
type SampleResult map[string]any

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

// GetAttributeNamesAndValues splits a SampleResult map into two slices and
// returns them. The first slice contains all the keys of SampleResult,
// representing the table's attribute names, and the second slice is the map's
// corresponding values.
func (result SampleResult) GetAttributeNamesAndValues() ([]string, []string) {
	names := make([]string, 0, len(result))
	vals := make([]string, 0, len(result))
	for name, val := range result {
		names = append(names, name)
		var v string
		if b, ok := val.([]byte); ok {
			v = string(b)
		} else {
			v = fmt.Sprint(val)
		}
		vals = append(vals, v)
	}
	return names, vals
}

// SampleRepository is a helper function which will sample every table in a
// given repository and return them as a collection of Sample. First the
// repository is introspected by calling sql.Introspect to return the
// repository metadata (Metadata). Then, for each schema and table in the
// metadata, it calls sql.SampleTable in a new goroutine. Once all the
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
				log.WithError(err).Errorf("error creating repository instance for database %s", db)
				return
			}
			// Close this repository and free up unused resources since we don't
			// need it any longer.
			defer func() { _ = repo.Close() }()
			s, err := SampleRepository(ctx, repo, sampleParams)
			if err != nil && len(s) == 0 {
				log.WithError(err).Errorf("error gathering repository data samples for database %s", db)
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
