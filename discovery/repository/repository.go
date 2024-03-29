package repository

import (
	"context"
	"errors"
	"fmt"

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

// Register makes a repository available by the provided repository type
// (repoType). If Register is called twice with the same repoType, or if
// constructor is nil, it panics. Note that Register is not thread-safe. It is
// expected to be called from either a package's init function, or from the
// program's main function.
func Register(repoType string, constructor RepoConstructor) {
	if constructor == nil {
		panic("attempt to register nil constructor for repoType " + repoType)
	}

	if _, dup := registry[repoType]; dup {
		panic("register called twice for repoType " + repoType)
	}

	registry[repoType] = constructor
}

// NewRepository is a factory function to return a concrete Repository
// implementation based on the specified type, e.g. MySQL, PostgreSQL, MSSQL,
// etc.
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
