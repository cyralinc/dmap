package sql

import (
	"context"
	"errors"
	"fmt"
)

var (
	// DefaultRegistry is the default, global repository registry used by the
	// package of which a number of convenience functions in this package act
	// on. All currently out-of-the-box repository types are registered to this
	// registry by this package's init function. Users who want to use custom
	// Repository implementations, or just avoid global state altogether, should
	// use their own instance of Registry, instead of using DefaultRegistry and
	// the corresponding convenience functions.
	DefaultRegistry = NewRegistry()
)

// Registry is a repository registry that maps repository types to their
// respective constructor functions. It is used to create new repository
// instances based on the repository type. It is not thread-safe.
type Registry struct {
	constructors map[string]RepoConstructor
}

// RepoConstructor represents the function signature that all repository
// implementations should use for their constructor functions.
type RepoConstructor func(ctx context.Context, cfg RepoConfig) (Repository, error)

// NewRegistry creates a new Registry instance.
func NewRegistry() *Registry {
	return &Registry{constructors: make(map[string]RepoConstructor)}
}

// Register makes a repository available by the provided repository type. If
// Register is called twice with the same repoType, or if constructor is nil, it
// returns an error. Note that Register is not thread-safe.
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

// Unregister removes a repository type from the registry. If the repository
// type is not registered, this method is a no-op. Note that Unregister is not
// thread-safe.
func (r *Registry) Unregister(repoType string) {
	delete(r.constructors, repoType)
}

// NewRepository is a factory method to return a concrete Repository
// implementation based on the specified type, e.g. MySQL, Postgres, SQL Server,
// etc., which must be registered with the registry. If the repository type is
// not registered, an error is returned. A new instance of the repository is
// returned each time this method is called. Note that NewRepository is not
// thread-safe.
func (r *Registry) NewRepository(ctx context.Context, repoType string, cfg RepoConfig) (Repository, error) {
	constructor, ok := r.constructors[repoType]
	if !ok {
		return nil, errors.New("unsupported repo type " + repoType)
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

// Unregister is a convenience function that delegates to DefaultRegistry. See
// Registry.Unregister for more details.
func Unregister(repoType string) {
	DefaultRegistry.Unregister(repoType)
}

// NewRepository is a convenience function that delegates to DefaultRegistry.
// See Registry.NewRepository for more details.
func NewRepository(ctx context.Context, repoType string, cfg RepoConfig) (Repository, error) {
	return DefaultRegistry.NewRepository(ctx, repoType, cfg)
}

// init registers all out-of-the-box repository types and their respective
// constructors with the DefaultRegistry.
func init() {
	MustRegister(
		RepoTypeDenodo,
		func(_ context.Context, cfg RepoConfig) (Repository, error) {
			return NewDenodoRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeMysql,
		func(_ context.Context, cfg RepoConfig) (Repository, error) {
			return NewMySqlRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeOracle,
		func(_ context.Context, cfg RepoConfig) (Repository, error) {
			return NewOracleRepository(cfg)
		},
	)
	MustRegister(
		RepoTypePostgres,
		func(_ context.Context, cfg RepoConfig) (Repository, error) {
			return NewPostgresRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeRedshift,
		func(_ context.Context, cfg RepoConfig) (Repository, error) {
			return NewRedshiftRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeSnowflake,
		func(ctx context.Context, cfg RepoConfig) (Repository, error) {
			return NewSnowflakeRepository(cfg)
		},
	)
	MustRegister(
		RepoTypeSqlServer,
		func(_ context.Context, cfg RepoConfig) (Repository, error) {
			return NewSqlServerRepository(cfg)
		},
	)
}
