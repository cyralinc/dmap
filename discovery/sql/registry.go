package sql

import (
	"context"
	"errors"
	"fmt"

	"github.com/cyralinc/dmap/discovery/config"
)

var (
	// TODO: godoc -ccampo 2024-04-02
	DefaultRegistry = NewRegistry()
)

// TODO: godoc -ccampo 2024-04-02
type Registry struct {
	constructors map[string]RepoConstructor
}

// RepoConstructor represents the function signature that all repository
// implementations should use for their constructor functions.
type RepoConstructor func(ctx context.Context, cfg config.RepoConfig) (Repository, error)

// TODO: godoc -ccampo 2024-04-02
func NewRegistry() *Registry {
	return &Registry{constructors: make(map[string]RepoConstructor)}
}

// Register makes a repository available by the provided repository type. If
// Register is called twice with the same repoType, or if constructor is nil, it
// returns an error. Note that Register is not thread-safe. It is
// expected to be called from either a package's init function, or from the
// program's main function.
func (r *Registry) Register(repoType string, constructor RepoConstructor) error {
	if constructor == nil {
		return fmt.Errorf("attempt to register nil constructor for repoType %s", repoType)
	}
	if r.constructors == nil {
		r.constructors = make(map[string]RepoConstructor)
	}
	if _, dup := r.constructors[repoType]; dup {
		return fmt.Errorf("register called twice for repoType %s", repoType)
	}
	r.constructors[repoType] = constructor
	return nil
}

// MustRegister is the same as Registry.Register, but panics if an error occurs.
func (r *Registry) MustRegister(repoType string, constructor RepoConstructor) {
	if err := r.Register(repoType, constructor); err != nil {
		panic(err)
	}
}

// NewRepository is a factory method to return a concrete Repository
// implementation based on the specified type, e.g. MySQL, PostgreSQL, MSSQL,
// etc.
func (r *Registry) NewRepository(ctx context.Context, cfg config.RepoConfig) (Repository, error) {
	constructor, ok := r.constructors[cfg.Type]
	if !ok {
		return nil, errors.New("unsupported repo type " + cfg.Type)
	}
	repo, err := constructor(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating repository, %w", err)
	}
	return repo, nil
}

// Register is a convenience function that delegates to DefaultRegistry. See
// Registry.Register for more details.
func Register(repoType string, constructor RepoConstructor) error {
	return DefaultRegistry.Register(repoType, constructor)
}

// MustRegister is a convenience function that delegates to DefaultRegistry. See
// Registry.MustRegister for more details.
func MustRegister(repoType string, constructor RepoConstructor) {
	DefaultRegistry.MustRegister(repoType, constructor)
}

// NewRepository is a convenience function that delegates to DefaultRegistry.
// See Registry.NewRepository for more details.
func NewRepository(ctx context.Context, cfg config.RepoConfig) (Repository, error) {
	return DefaultRegistry.NewRepository(ctx, cfg)
}

// TODO: godoc -ccampo 2024-04-02
func init() {
	MustRegister(
		RepoTypeDenodo,
		func(_ context.Context, cfg config.RepoConfig) (Repository, error) {
			return NewDenodoRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeMysql,
		func(_ context.Context, cfg config.RepoConfig) (Repository, error) {
			return NewMySqlRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeOracle,
		func(_ context.Context, cfg config.RepoConfig) (Repository, error) {
			return NewOracleRepository(cfg)
		},
	)
	MustRegister(
		RepoTypePostgres,
		func(_ context.Context, cfg config.RepoConfig) (Repository, error) {
			return NewPostgresRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeRedshift,
		func(_ context.Context, cfg config.RepoConfig) (Repository, error) {
			return NewRedshiftRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeSnowflake,
		func(ctx context.Context, cfg config.RepoConfig) (Repository, error) {
			return NewSnowflakeRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeSqlServer,
		func(_ context.Context, cfg config.RepoConfig) (Repository, error) {
			return NewSqlServerRepository(cfg)
		},
	)
}
