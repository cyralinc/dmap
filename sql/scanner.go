package sql

import (
	"context"
	"fmt"

	"github.com/gobwas/glob"
	log "github.com/sirupsen/logrus"

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
	Config     ScannerConfig
	labels     []classification.Label
	classifier classification.Classifier
}

// RepoScanner implements the scan.RepoScanner interface.
var _ scan.RepoScanner = (*Scanner)(nil)

// NewScanner creates a new Scanner instance with the provided configuration.
func NewScanner(cfg ScannerConfig) (*Scanner, error) {
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
	c, err := classification.NewLabelClassifier(lbls...)
	if err != nil {
		return nil, fmt.Errorf("error creating new label classifier: %w", err)
	}
	return &Scanner{Config: cfg, labels: lbls, classifier: c}, nil
}

// Scan performs the data repository scan. It introspects and samples the
// repository, classifies the sampled data, and publishes the results to the
// configured classification publisher.
func (s *Scanner) Scan(ctx context.Context) (*scan.RepoScanResults, error) {
	// Introspect and sample the data repository.
	samples, err := s.sample(ctx)
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
	classifications, err := classifySamples(ctx, samples, s.classifier)
	if err != nil {
		return nil, fmt.Errorf("error classifying samples: %w", err)
	}
	return &scan.RepoScanResults{
		Labels:          s.labels,
		Classifications: classifications,
	}, nil
}

func (s *Scanner) sample(ctx context.Context) ([]Sample, error) {
	// This closure is used to create a new repository instance for each
	// database that is sampled. When there are multiple databases to sample,
	// it is passed to sampleAllDbs to create the necessary repository instances
	// for each database. When there is only a single database to sample, it is
	// used directly below to create the repository instance for that database,
	// which is passed to sampleDb to sample the database.
	newRepo := func(ctx context.Context, cfg RepoConfig) (Repository, error) {
		return s.Config.Registry.NewRepository(ctx, s.Config.RepoType, cfg)
	}
	introspectParams := IntrospectParameters{
		IncludePaths: s.Config.IncludePaths,
		ExcludePaths: s.Config.ExcludePaths,
	}
	// Check if the user specified a single database, or told us to scan an
	// Oracle DB. In that case, therefore we only need to sample that single
	// database. Note that Oracle doesn't really have the concept of
	// "databases", therefore a single repository instance will always scan the
	// entire database.
	if s.Config.RepoConfig.Database != "" || s.Config.RepoType == RepoTypeOracle {
		repo, err := newRepo(ctx, s.Config.RepoConfig)
		if err != nil {
			return nil, fmt.Errorf("error creating repository: %w", err)
		}
		defer func() { _ = repo.Close() }()
		return sampleDb(ctx, repo, introspectParams, s.Config.SampleSize, s.Config.Offset)
	}
	// The name of the database to connect to has been left unspecified by the
	// user, so we try to connect and sample all databases instead.
	return sampleAllDbs(
		ctx,
		newRepo,
		s.Config.RepoConfig,
		introspectParams,
		s.Config.SampleSize,
		s.Config.Offset,
	)
}
