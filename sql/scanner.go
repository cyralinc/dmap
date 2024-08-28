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

// Pair type intended to be passed to a channel (see sampleAllDbs).
type samplesAndErr struct {
	samples []Sample
	err     error
}

// Pair type intended to be passed to a channel (see sampleDb).
type sampleAndErr struct {
	sample Sample
	err    error
}

// ScannerConfig is the configuration for the Scanner.
type ScannerConfig struct {
	RepoType                   string
	RepoConfig                 RepoConfig
	Registry                   *Registry
	IncludePaths, ExcludePaths []glob.Glob
	SampleSize                 uint
	Offset                     uint
	LabelsYamlFilename         string
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
	// Load the data labels - either from the predefined (embedded) labels or
	// from a provided custom labels file.
	var (
		lbls []classification.Label
		err  error
	)
	if cfg.LabelsYamlFilename == "" {
		// Use the predefined labels if the user didn't specify a labels file.
		lbls, err = classification.GetPredefinedLabels()
	} else {
		// Load the labels from the specified file.
		lbls, err = classification.GetCustomLabels(cfg.LabelsYamlFilename)
	}
	if err != nil {
		errMsg := "error(s) loading data labels"
		// This error means that some labels weren't loaded due to having
		// invalid classification rules. We only log a warning in this case,
		// since we still want to proceed with the labels that were
		// successfully loaded.
		var errs classification.InvalidLabelsError
		if errors.As(err, &errs) {
			if len(lbls) == 0 {
				return nil, fmt.Errorf("%s; no labels were loaded: %w", errMsg, err)
			}
			log.WithError(errs).Warnf("%s: some labels were not loaded", errMsg)
		} else {
			return nil, fmt.Errorf("%s: %w", errMsg, err)
		}
	}
	// Create a new label classifier with the embedded labels.
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
	introspectCtx := ctx
	if s.config.RepoConfig.QueryTimeout > 0 {
		var cancel context.CancelFunc
		introspectCtx, cancel = context.WithTimeout(ctx, s.config.RepoConfig.QueryTimeout)
		defer cancel()
	}
	introspectParams := IntrospectParameters{
		IncludePaths: s.config.IncludePaths,
		ExcludePaths: s.config.ExcludePaths,
	}
	meta, err := repo.Introspect(introspectCtx, introspectParams)
	if err != nil {
		return nil, fmt.Errorf("error introspecting repository: %w", err)
	}
	// This goroutine launches additional goroutines, one for each table, which
	// sample the respective tables and send the results to the out channel. A
	// semaphore is optionally used to limit the number of tables that are
	// sampled concurrently. We do this on a dedicated goroutine so we can
	// immediately read from the out channel on this goroutine, and avoid
	// possible deadlocks due to the semaphore.
	out := make(chan sampleAndErr)
	go func() {
		// Before we return, wait for all the goroutines we launch below to
		// complete, and then close the out channel once they're all done so the
		// main goroutine can aggregate the results and return them.
		var wg sync.WaitGroup
		defer func() { wg.Wait(); close(out) }()
		// Optionally use a semaphore to limit the number of tables sampled
		// concurrently.
		var sema *semaphore.Weighted
		if s.config.RepoConfig.MaxConcurrency > 0 {
			sema = semaphore.NewWeighted(int64(s.config.RepoConfig.MaxConcurrency))
		}
		for _, schemaMeta := range meta.Schemas {
			for _, tableMeta := range schemaMeta.Tables {
				if sema != nil {
					// Acquire a semaphore slot before launching a goroutine to
					// sample the table. This will block if the semaphore is
					// full, and will unblock once a slot is available. An error
					// means the context was cancelled.
					if err := sema.Acquire(ctx, 1); err != nil {
						log.WithError(err).Error("error acquiring semaphore")
						return
					}
				}
				wg.Add(1)
				// Launch a goroutine to sample the table.
				go func(ctx context.Context, meta *TableMetadata) {
					defer func() {
						if sema != nil {
							// Release the slot once the goroutine is done.
							sema.Release(1)
						}
						wg.Done()
					}()
					sampleCtx := ctx
					if s.config.RepoConfig.QueryTimeout > 0 {
						var cancel context.CancelFunc
						sampleCtx, cancel = context.WithTimeout(ctx, s.config.RepoConfig.QueryTimeout)
						defer cancel()
					}
					params := SampleParameters{
						Metadata:   meta,
						SampleSize: s.config.SampleSize,
						Offset:     s.config.Offset,
					}
					sample, err := repo.SampleTable(sampleCtx, params)
					select {
					case <-ctx.Done():
					case out <- sampleAndErr{sample: sample, err: err}:
					}
				}(ctx, tableMeta)
			}
		}
	}()

	// Aggregate and return the results.
	var samples []Sample
	var errs error
	for {
		select {
		case <-ctx.Done():
			errs = errors.Join(errs, ctx.Err())
			return samples, fmt.Errorf("error(s) sampling repository: %w", errs)
		case res, ok := <-out:
			if !ok {
				// The out channel has been closed, so we're done.
				if errs != nil {
					return samples, fmt.Errorf("error(s) sampling repository: %w", errs)
				}
				return samples, nil
			}
			if res.err != nil {
				errs = errors.Join(errs, res.err)
			} else {
				samples = append(samples, res.sample)
			}
		}
	}
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
	listDbCtx := ctx
	if s.config.RepoConfig.QueryTimeout > 0 {
		var cancel context.CancelFunc
		listDbCtx, cancel = context.WithTimeout(ctx, s.config.RepoConfig.QueryTimeout)
		defer cancel()
	}
	dbs, err := repo.ListDatabases(listDbCtx)
	if err != nil {
		return nil, fmt.Errorf("error listing databases: %w", err)
	}

	// This goroutine launches additional goroutines, one for each database,
	// which sample the respective databases and send the results to the out
	// channel. A semaphore is optionally used to limit the number of databases
	// sampled concurrently. We do this on a dedicated goroutine so we can
	// immediately read from the out channel on this goroutine, and avoid
	// possible deadlocks due to the semaphore.
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	out := make(chan samplesAndErr)
	go func() {
		// Before we return, wait for all the goroutines we launch below to
		// complete, and then close the out channel once they're all done so the
		// main goroutine can aggregate the results and return them.
		var wg sync.WaitGroup
		defer func() { wg.Wait(); close(out) }()
		// Optionally use a semaphore to limit the number of databases sampled
		// concurrently.
		var sema *semaphore.Weighted
		if s.config.RepoConfig.MaxParallelDbs > 0 {
			sema = semaphore.NewWeighted(int64(s.config.RepoConfig.MaxParallelDbs))
		}
		for _, db := range dbs {
			if sema != nil {
				// Acquire a semaphore slot before launching a goroutine to
				// sample the database. This will block if the semaphore is
				// full, and will unblock once a slot is available. An error
				// means the context was cancelled.
				if err := sema.Acquire(ctx, 1); err != nil {
					log.WithError(err).Error("error acquiring semaphore")
					return
				}
			}
			// Launch a goroutine to sample the database.
			wg.Add(1)
			go func(db string, cfg RepoConfig) {
				defer func() {
					if sema != nil {
						// Release the slot once the goroutine is done.
						sema.Release(1)
					}
					wg.Done()
				}()
				// Sample this specific database.
				samples, err := s.sampleDb(ctx, db)
				if err != nil && len(samples) == 0 {
					log.WithError(err).Errorf("error gathering repository data samples for database %s", db)
					return
				}
				// Send the samples for this database to the 'out' channel. The
				// samples for each database will be aggregated into a single
				// slice on the main goroutine and returned.
				select {
				case <-ctx.Done():
				case out <- samplesAndErr{samples: samples, err: err}:
				}
			}(db, s.config.RepoConfig)
		}
	}()

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
				// We received an error while classifying the sample. If we
				// didn't get any results, return an error. Otherwise, log a
				// warning and continue with the partial results.
				if len(res) == 0 {
					return nil, fmt.Errorf("error(s) classifying sample: %w", err)
				}
				log.WithError(err).Warn("error(s) classifying sample, continuing with partial results")
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
