package scan

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/cyralinc/dmap/classification"
	"github.com/cyralinc/dmap/discovery"
)

// RepoScannerConfig is the configuration for the RepoScanner.
type RepoScannerConfig struct {
	Dmap DmapConfig           `embed:""`
	Repo discovery.RepoConfig `embed:""`
}

// DmapConfig is the necessary configuration to connect to the Dmap API.
type DmapConfig struct {
	ApiBaseUrl   string `help:"Base URL of the Dmap API." default:"https://api.dmap.cyral.io"`
	ClientID     string `help:"API client ID to access the Dmap API."`
	ClientSecret string `help:"API client secret to access the Dmap API."` //#nosec G101 -- false positive
}

// RepoScanner is a data discovery scanner that scans a data repository for
// sensitive data. It also classifies the data and publishes the results to
// the configured external sources. It currently only supports SQL-based
// repositories.
type RepoScanner struct {
	config     RepoScannerConfig
	repository discovery.SQLRepository
	classifier classification.Classifier
	publisher  classification.Publisher
}

// RepoScannerOption is a functional option type for the RepoScanner type.
type RepoScannerOption func(*RepoScanner)

// WithSQLRepository is a functional option that sets the SQLRepository for the
// RepoScanner.
func WithSQLRepository(r discovery.SQLRepository) RepoScannerOption {
	return func(s *RepoScanner) { s.repository = r }
}

// WithClassifier is a functional option that sets the classifier for the
// RepoScanner.
func WithClassifier(c classification.Classifier) RepoScannerOption {
	return func(s *RepoScanner) { s.classifier = c }
}

// WithPublisher is a functional option that sets the publisher for the RepoScanner.
func WithPublisher(p classification.Publisher) RepoScannerOption {
	return func(s *RepoScanner) { s.publisher = p }
}

// NewRepoScanner creates a new RepoScanner instance with the provided configuration.
func NewRepoScanner(ctx context.Context, cfg RepoScannerConfig, opts ...RepoScannerOption) (*RepoScanner, error) {
	s := &RepoScanner{config: cfg}
	// Apply options.
	for _, opt := range opts {
		opt(s)
	}
	if s.publisher == nil {
		// Default to stdout publisher.
		s.publisher = classification.NewStdOutPublisher()
	}
	if s.classifier == nil {
		// Create a new label classifier with the embedded labels.
		lbls, err := classification.GetEmbeddedLabels()
		if err != nil {
			return nil, fmt.Errorf("error getting embedded labels: %w", err)
		}
		c, err := classification.NewLabelClassifier(lbls.ToSlice()...)
		if err != nil {
			return nil, fmt.Errorf("error creating new label classifier: %w", err)
		}
		s.classifier = c
	}
	if s.repository == nil {
		// Get a repository instance from the default registry.
		repo, err := discovery.NewRepository(ctx, s.config.Repo)
		if err != nil {
			return nil, fmt.Errorf("error connecting to database: %w", err)
		}
		s.repository = repo
	}
	return s, nil
}

// Scan performs the data repository scan. It introspects and samples the
// repository, classifies the sampled data, and publishes the results to the
// configured classification publisher.
func (s *RepoScanner) Scan(ctx context.Context) error {
	sampleParams := discovery.SampleParameters{SampleSize: s.config.Repo.SampleSize}
	var samples []discovery.Sample
	// The name of the database to connect to has been left unspecified by
	// the user, so we try to connect and sample all databases instead. Note
	// that Oracle doesn't really have the concept of "databases", and thus
	// the RepoScanner always scans the entire database, so only the single
	// (default) repository instance is required in that case.
	if s.config.Repo.Database == "" && s.config.Repo.Type != discovery.RepoTypeOracle {
		var err error
		samples, err = discovery.SampleAllDatabases(
			ctx,
			s.repository,
			s.config.Repo,
			sampleParams,
		)
		if err != nil {
			err = fmt.Errorf("error sampling databases: %w", err)
			// If we didn't get any samples, just return the error.
			if len(samples) == 0 {
				return err
			}
			// There were error(s) during sampling, but we still got some
			// samples. Just warn and continue.
			log.WithError(err).Warn("error sampling databases")
		}
	} else {
		// User specified a database (or this is an Oracle DB), therefore
		// we already have a repository instance for it. Just use it to
		// sample that database only.
		var err error
		samples, err = discovery.SampleRepository(ctx, s.repository, sampleParams)
		if err != nil {
			err = fmt.Errorf("error gathering repository data samples: %w", err)
			// If we didn't get any samples, just return the error.
			if len(samples) == 0 {
				return err
			}
			// There were error(s) during sampling, but we still got some
			// samples. Just warn and continue.
			log.WithError(err).Warn("error gathering repository data samples")
		}
	}

	// Classify sampled data
	classifications, err := classifySamples(ctx, samples, s.classifier)
	if err != nil {
		return fmt.Errorf("error classifying samples: %w", err)
	}

	// Publish classifications if necessary
	if len(classifications) == 0 {
		log.Info("No discovered classifications")
	} else if err := s.publisher.PublishClassifications(ctx, s.config.Repo.Host, classifications); err != nil {
		return fmt.Errorf("error publishing classifications: %w", err)
	}

	// Done!
	return nil
}

// Cleanup performs cleanup operations for the RepoScanner.
func (s *RepoScanner) Cleanup() {
	// Nil checks are prevent panics if deps are not yet initialized.
	if s.repository != nil {
		_ = s.repository.Close()
	}
}

// classifySamples uses the provided classifiers to classify the sample data
// passed via the "samples" parameter. It is mostly a helper function which
// loops through each repository.Sample, retrieves the attribute names and
// values of that sample, passes them to Classifier.Classify, and then
// aggregates the results. Please see the documentation for Classifier and its
// Classify method for more details. The returned slice represents all the
// unique classification results for a given sample set.
func classifySamples(
	ctx context.Context,
	samples []discovery.Sample,
	classifier classification.Classifier,
) ([]classification.ClassifiedTable, error) {
	tables := make([]classification.ClassifiedTable, 0, len(samples))
	for _, sample := range samples {
		// Classify each sampled row and combine the results.
		result := make(classification.Result)
		for _, sampleResult := range sample.Results {
			res, err := classifier.Classify(ctx, sampleResult)
			if err != nil {
				return nil, fmt.Errorf("error classifying sample: %w", err)
			}
			result.Merge(res)
		}
		if len(result) > 0 {
			table := classification.ClassifiedTable{
				Repo:            sample.Metadata.Repo,
				Database:        sample.Metadata.Database,
				Schema:          sample.Metadata.Schema,
				Table:           sample.Metadata.Table,
				Classifications: result,
			}
			tables = append(tables, table)
		}
	}
	return tables, nil
}
