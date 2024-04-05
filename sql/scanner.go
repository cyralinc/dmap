package sql

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/gobwas/glob"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	"github.com/cyralinc/dmap/classification"
	"github.com/cyralinc/dmap/scan"
)

// ScannerConfig is the configuration for the Scanner.
type ScannerConfig struct {
	RepoType                   string
	RepoConfig                 RepoConfig
	Registry                   *Registry
	IncludePaths, ExcludePaths []glob.Glob
	SampleSize                 uint
	Offset                     uint
}

// Scanner is a data discovery scanner that scans a data repository for
// sensitive data. It also classifies the data and publishes the results to
// the configured external sources. It currently only supports SQL-based
// repositories.
type Scanner struct {
	config     ScannerConfig
	labels     []classification.Label
	classifier classification.Classifier
}

// RepoScanner implements the scan.RepoScanner interface.
var _ scan.RepoScanner = (*Scanner)(nil)

// NewScanner creates a new Scanner instance with the provided configuration.
func NewScanner(ctx context.Context, cfg ScannerConfig) (*Scanner, error) {
	if cfg.RepoType == "" {
		return nil, fmt.Errorf("repository type not specified")
	}
	if cfg.Registry == nil {
		cfg.Registry = DefaultRegistry
	}
	// Create a new label classifier with the embedded labels.
	lbls, err := classification.GetEmbeddedLabels()
	if err != nil {
		return nil, fmt.Errorf("error getting embedded labels: %w", err)
	}
	c, err := classification.NewLabelClassifier(ctx, lbls...)
	if err != nil {
		return nil, fmt.Errorf("error creating new label classifier: %w", err)
	}
	return &Scanner{config: cfg, labels: lbls, classifier: c}, nil
}

// Scan performs the data repository scan. It introspects and samples the
// repository, classifies the sampled data, and publishes the results to the
// configured classification publisher.
func (s *Scanner) Scan(ctx context.Context) (*scan.RepoScanResults, error) {
	// First introspect and sample the data repository.
	var (
		samples []Sample
		err     error
	)
	// Check if the user specified a single database, or told us to scan an
	// Oracle DB. In that case, therefore we only need to sample that single
	// database. Note that Oracle doesn't really have the concept of
	// "databases", therefore a single repository instance will always scan the
	// entire database.
	if s.config.RepoConfig.Database != "" || s.config.RepoType == RepoTypeOracle {
		samples, err = s.sampleDb(ctx, s.config.RepoConfig.Database)
	} else {
		// The name of the database to connect to has been left unspecified by
		// the user, so we try to connect and sample all databases instead.
		samples, err = s.sampleAllDbs(ctx)
	}
	if err != nil {
		msg := "error sampling repository"
		// If we didn't get any samples, just return the error.
		if len(samples) == 0 {
			return nil, fmt.Errorf("%s: %w", msg, err)
		}
		// There were error(s) during sampling, but we still got some samples.
		// Just warn and continue.
		log.WithError(err).Warn(msg)
	}
	// Classify the sampled data.
	classifications, err := s.classifySamples(ctx, samples)
	if err != nil {
		return nil, fmt.Errorf("error classifying samples: %w", err)
	}
	return &scan.RepoScanResults{
		Labels:          s.labels,
		Classifications: classifications,
	}, nil
}

// sampleDb is samples every table in a given database and returns the samples.
// The repository instance is created with the provided database name by
// newRepository. The database is then introspected by calling
// Repository.Introspect to return the repository metadata (Metadata). Then, for
// each schema and table in the metadata, it calls Repository.SampleTable in a
// new goroutine to sample all tables concurrently. Note however that the level
// of concurrency should be limited by the max number of open connections
// specified for the scanner, since the underlying repository should respect
// this across goroutines. This of course depends on the implementation, however
// for all the out-of-the-box Repository implementations, this applies. Once all
// the sampling goroutines are finished, their results are collected and
// returned as a slice of Sample.
func (s *Scanner) sampleDb(ctx context.Context, db string) ([]Sample, error) {
	// Create the repository instance that will be used to sample the database.
	cfg := s.config.RepoConfig
	cfg.Database = db
	repo, err := s.newRepository(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating repository instance: %w", err)
	}
	defer func() { _ = repo.Close() }()
	// Introspect the repository to get the metadata.
	introspectParams := IntrospectParameters{
		IncludePaths: s.config.IncludePaths,
		ExcludePaths: s.config.ExcludePaths,
	}
	meta, err := repo.Introspect(ctx, introspectParams)
	if err != nil {
		return nil, fmt.Errorf("error introspecting repository: %w", err)
	}
	// This is a "pair" type intended to be passed to the channel below.
	type sampleAndErr struct {
		sample Sample
		err    error
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
					SampleSize: s.config.SampleSize,
					Offset:     s.config.Offset,
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
		case res := <-out:
			if res.err != nil {
				errs = errors.Join(errs, res.err)
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

// sampleAllDbs samples all the databases on the server. It samples each
// database concurrently by calling sampleDb for each database on a new
// goroutine. It first creates a new Repository instance by calling
// newRepository. This repository is intended to be configured to connect to the
// default database on the server, or at least some database which can be used
// to enumerate the full set of databases on the server. An error will be
// returned if the set of databases cannot be listed. If there is an error
// connecting to or sampling a database, the error will be logged and no samples
// will be returned for that database. Therefore, the returned slice of samples
// contains samples for only the databases which could be discovered and
// successfully sampled, and could potentially be empty if no databases were
// sampled.
func (s *Scanner) sampleAllDbs(ctx context.Context) ([]Sample, error) {
	// Create a repository instance that will be used to list all the databases
	// on the server.
	repo, err := s.newRepository(ctx, s.config.RepoConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating repository instance: %w", err)
	}
	defer func() { _ = repo.Close() }()

	// We assume that this repository will be connected to the default database
	// (or at least some database that can discover all the other databases).
	// Use it to discover all the other databases on the server.
	dbs, err := repo.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing databases: %w", err)
	}

	// Sample each database on a separate goroutine, and send the samples to
	// the 'out' channel. Each slice of samples will be aggregated below on this
	// goroutine and returned.
	var wg sync.WaitGroup
	// This is a "pair" type intended to be passed to the channel below.
	type samplesAndErr struct {
		samples []Sample
		err     error
	}
	out := make(chan samplesAndErr)
	wg.Add(len(dbs))
	// Using a semaphore here ensures that we avoid opening more than the
	// specified total number of connections, since we end up creating multiple
	// database handles (one per database).
	var sema *semaphore.Weighted
	if s.config.RepoConfig.MaxOpenConns > 0 {
		sema = semaphore.NewWeighted(int64(s.config.RepoConfig.MaxOpenConns))
	}
	for _, db := range dbs {
		go func(db string, cfg RepoConfig) {
			defer wg.Done()
			if sema != nil {
				_ = sema.Acquire(ctx, 1)
				defer sema.Release(1)
			}
			// Sample this specific database.
			samples, err := s.sampleDb(ctx, db)
			if err != nil && len(samples) == 0 {
				log.WithError(err).Errorf("error gathering repository data samples for database %s", db)
				return
			}
			// Send the samples for this database to the 'out' channel. The
			// samples for each database will be aggregated into a single slice
			// on the main goroutine and returned.
			select {
			case <-ctx.Done():
				return
			case out <- samplesAndErr{samples: samples, err: err}:
			}
		}(db, s.config.RepoConfig)
	}

	// Start a goroutine to close the 'out' channel once all the goroutines we
	// launched above are done. This will allow the aggregation range loop below
	// to terminate properly. Note that this must start after the wg.Add call.
	// See https://go.dev/blog/pipelines ("Fan-out, fan-in" section).
	go func() { wg.Wait(); close(out) }()

	// Aggregate and return the results.
	var ret []Sample
	var errs error
	for {
		select {
		case <-ctx.Done():
			return ret, errors.Join(errs, ctx.Err())
		case res, ok := <-out:
			if !ok {
				// The 'out' channel has been closed, so we're done.
				return ret, errs
			}
			ret = append(ret, res.samples...)
			if res.err != nil {
				errs = errors.Join(errs, res.err)
			}
		}
	}
}

// classifySamples uses the scanner's classifier to classify the provided slice
// of samples. Each sampled row is individually classified. The returned slice
// of classifications represents all the UNIQUE classifications for a given
// sample set.
func (s *Scanner) classifySamples(
	ctx context.Context,
	samples []Sample,
) ([]classification.Classification, error) {
	uniqueClassifications := make(map[string]classification.Classification)
	for _, sample := range samples {
		// Classify each sampled row and combine the classifications.
		for _, sampleResult := range sample.Results {
			res, err := s.classifier.Classify(ctx, sampleResult)
			if err != nil {
				return nil, fmt.Errorf("error classifying sample: %w", err)
			}
			for attr, labels := range res {
				attrPath := append(sample.TablePath, attr)
				// U+2063 is an invisible separator. It is used here to ensure
				// that the path key is unique and does not conflict with any of
				// the path elements.
				key := strings.Join(attrPath, "\u2063")
				result, ok := uniqueClassifications[key]
				if !ok {
					uniqueClassifications[key] = classification.Classification{
						AttributePath: attrPath,
						Labels:        labels,
					}
				} else {
					// Merge the labels from the new result into the existing result.
					maps.Copy(result.Labels, labels)
				}
			}
		}
	}
	// Convert the map of unique classifications to a slice.
	classifications := make([]classification.Classification, 0, len(uniqueClassifications))
	for _, result := range uniqueClassifications {
		classifications = append(classifications, result)
	}
	return classifications, nil
}

// newRepository creates a new Repository instance with the provided
// configuration. It delegates the actual creation of the repository to the
// scanner's Registry.NewRepository method, using the scanner's RepoType and
// the provided configuration. You may wonder why we just don't use the
// scanner's repo configuration directly (i.e. s.config.RepoConfig) instead of
// passing it as an argument. The reason is that we want to be able to create
// a new repository instance with a different configuration than the one
// specified in the scanner's configuration. This is useful when we want to
// sample a specific database, for example, and we want to create a new
// repository instance with the database name set to that specific database.
func (s *Scanner) newRepository(ctx context.Context, cfg RepoConfig) (Repository, error) {
	return s.config.Registry.NewRepository(ctx, s.config.RepoType, cfg)
}
