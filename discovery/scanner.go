package discovery

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/cyralinc/dmap/classification"
	"github.com/cyralinc/dmap/classification/publisher"
	"github.com/cyralinc/dmap/discovery/config"
	"github.com/cyralinc/dmap/discovery/repository"
	"github.com/cyralinc/dmap/discovery/repository/oracle"

	// Registers repo types for use via 'init' side effects
	_ "github.com/cyralinc/dmap/discovery/repository/denodo"
	_ "github.com/cyralinc/dmap/discovery/repository/mysql"
	_ "github.com/cyralinc/dmap/discovery/repository/oracle"
	_ "github.com/cyralinc/dmap/discovery/repository/postgresql"
	_ "github.com/cyralinc/dmap/discovery/repository/redshift"
	_ "github.com/cyralinc/dmap/discovery/repository/snowflake"
	_ "github.com/cyralinc/dmap/discovery/repository/sqlserver"
)

// Scanner is a data discovery scanner that scans a data repository for
// sensitive data. It also classifies the data and publishes the results to
// the configured external sources.
type Scanner struct {
	config         *config.Config
	repository     repository.Repository
	embeddedLabels []classification.Label
	customLabels   []classification.Label
	classifier     *classification.LabelClassifier
	publisher      publisher.Publisher
	initialized    bool
}

// NewScanner creates a new Scanner instance with the given configuration.
func NewScanner(config *config.Config) Scanner {
	return Scanner{config: config}
}

// Init initializes the Scanner instance. It must be called before calling Run.
func (s *Scanner) Init(ctx context.Context) error {
	if s.config == nil {
		return errors.New("unable to start crawler: config not found")
	}
	// Note: order is important here because we don't have nil checks in these
	// init methods.
	s.initPublisher()
	if err := s.initEmbeddedLabels(); err != nil {
		return err
	}
	if err := s.initLabelClassifier(); err != nil {
		return err
	}
	if err := s.initRepository(ctx); err != nil {
		return err
	}
	// Done!
	s.initialized = true
	return nil
}

// Run runs the data repository scan. It samples the data repository, classifies
// the data, and publishes the results to the configured external sources.
func (s *Scanner) Run(ctx context.Context) error {
	if !s.initialized {
		return errors.New("scanner not initialized")
	}
	sampleParams := repository.SampleParameters{SampleSize: s.config.Repo.SampleSize}
	var samples []repository.Sample
	// The name of the database to connect to has been left unspecified by
	// the user, so we try to connect and sample all databases instead. Note
	// that Oracle doesn't really have the concept of "databases", and thus
	// the Scanner always scans the entire database, so only the single
	// (default) repository instance is required in that case.
	if s.config.Repo.Database == "" && s.config.Repo.Type != oracle.RepoTypeOracle {
		var err error
		samples, err = repository.SampleAllDatabases(
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
		samples, err = repository.SampleRepository(ctx, s.repository, sampleParams)
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

// InitAndRun initializes the Scanner instance and runs the scan. It is a
// convenience method that calls Init followed by Run.
func (s *Scanner) InitAndRun(ctx context.Context) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	return s.Run(ctx)
}

func (s *Scanner) Cleanup() {
	// Nil checks are prevent panics if deps are not yet initialized.
	if s.repository != nil {
		_ = s.repository.Close()
	}
}

func (s *Scanner) initEmbeddedLabels() error {
	lbls, err := classification.GetEmbeddedLabels()
	if err != nil {
		return fmt.Errorf("error getting embedded labels: %w", err)
	}
	s.embeddedLabels = lbls.ToSlice()
	return nil
}

func (s *Scanner) initLabelClassifier() error {
	lbls := append(s.embeddedLabels, s.customLabels...)
	c, err := classification.NewLabelClassifier(lbls...)
	if err != nil {
		return fmt.Errorf("error creating label classifier: %w", err)
	}
	s.classifier = c
	return nil
}

func (s *Scanner) initPublisher() {
	s.publisher = publisher.NewStdOutPublisher()
}

func (s *Scanner) initRepository(ctx context.Context) error {
	// Connect to database
	repo, err := repository.NewRepository(ctx, s.config.Repo)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	s.repository = repo
	return nil
}
