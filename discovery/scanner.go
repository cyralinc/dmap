package discovery

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/cyralinc/dmap/classification"
	"github.com/cyralinc/dmap/classification/publisher"
	"github.com/cyralinc/dmap/discovery/config"
	"github.com/cyralinc/dmap/discovery/sql"
)

// Scanner is a data discovery scanner that scans a data repository for
// sensitive data. It also classifies the data and publishes the results to
// the configured external sources.
type Scanner struct {
	config     *config.Config
	repository sql.Repository
	classifier classification.Classifier
	publisher  publisher.Publisher
}

// ScannerOption is a functional option type for the Scanner type.
type ScannerOption func(*Scanner)

// Repository is a functional option that sets the repository for the Scanner.
func Repository(r sql.Repository) ScannerOption {
	return func(s *Scanner) { s.repository = r }
}

// Classifier is a functional option that sets the classifier for the Scanner.
func Classifier(c classification.Classifier) ScannerOption {
	return func(s *Scanner) { s.classifier = c }
}

// Publisher is a functional option that sets the publisher for the Scanner.
func Publisher(p publisher.Publisher) ScannerOption {
	return func(s *Scanner) { s.publisher = p }
}

// NewScanner creates a new Scanner instance with the provided configuration.
func NewScanner(
	ctx context.Context,
	config *config.Config,
	opts ...ScannerOption,
) (*Scanner, error) {
	if config == nil {
		return nil, errors.New("config can't be nil")
	}
	s := &Scanner{config: config}
	// Apply options.
	for _, opt := range opts {
		opt(s)
	}
	if s.publisher == nil {
		// Default to stdout publisher.
		s.publisher = publisher.NewStdOutPublisher()
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
		repo, err := sql.NewRepository(ctx, s.config.Repo)
		if err != nil {
			return nil, fmt.Errorf("error connecting to database: %w", err)
		}
		s.repository = repo
	}
	return s, nil
}

// Scan performs the data repository scan. It introspects and samples the
// repository, classifies the sampled data, and publishes the results to the
// configured publisher.
func (s *Scanner) Scan(ctx context.Context) error {
	sampleParams := sql.SampleParameters{SampleSize: s.config.Repo.SampleSize}
	var samples []sql.Sample
	// The name of the database to connect to has been left unspecified by
	// the user, so we try to connect and sample all databases instead. Note
	// that Oracle doesn't really have the concept of "databases", and thus
	// the Scanner always scans the entire database, so only the single
	// (default) repository instance is required in that case.
	if s.config.Repo.Database == "" && s.config.Repo.Type != sql.RepoTypeOracle {
		var err error
		samples, err = sql.SampleAllDatabases(
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
		samples, err = sql.SampleRepository(ctx, s.repository, sampleParams)
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
	classifications, err := classification.ClassifySamples(ctx, samples, s.classifier)
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

// Cleanup performs cleanup operations for the Scanner.
func (s *Scanner) Cleanup() {
	// Nil checks are prevent panics if deps are not yet initialized.
	if s.repository != nil {
		_ = s.repository.Close()
	}
}
