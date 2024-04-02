package discovery

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: refactor tests to use registry instance, not default registry -ccampo 2024-04-02

func TestRegistry_Register_Successful(t *testing.T) {
	repoType := "repoType"
	constructor := func(context.Context, RepoConfig) (SQLRepository, error) {
		return nil, nil
	}
	reg := NewRegistry()
	err := reg.Register(repoType, constructor)
	require.NoError(t, err)
	assert.Contains(t, reg.constructors, repoType)
}

func TestRegistry_MustRegister_NilConstructor(t *testing.T) {
	reg := NewRegistry()
	assert.Panics(t, func() { reg.MustRegister("repoType", nil) })
}

func TestRegistry_MustRegister_TwoCalls_Panics(t *testing.T) {
	repoType := "repoType"
	constructor := func(context.Context, RepoConfig) (SQLRepository, error) {
		return nil, nil
	}
	reg := NewRegistry()
	reg.MustRegister(repoType, constructor)
	assert.Contains(t, reg.constructors, repoType)
	assert.Panics(t, func() { reg.MustRegister(repoType, constructor) })
}

func TestRegistry_NewRepository_IsSuccessful(t *testing.T) {
	repoType := "repoType"
	called := false
	expectedRepo := dummyRepo{}
	constructor := func(context.Context, RepoConfig) (SQLRepository, error) {
		called = true
		return expectedRepo, nil
	}
	reg := NewRegistry()
	err := reg.Register(repoType, constructor)
	require.NoError(t, err)
	assert.Contains(t, reg.constructors, repoType)

	cfg := RepoConfig{Type: repoType}
	repo, err := reg.NewRepository(context.Background(), cfg)
	assert.NoError(t, err)
	assert.Equal(t, expectedRepo, repo)
	assert.True(t, called, "Constructor was not called")
}

func TestRegistry_NewRepository_ConstructorError(t *testing.T) {
	repoType := "repoType"
	called := false
	expectedErr := errors.New("dummy error")
	constructor := func(context.Context, RepoConfig) (SQLRepository, error) {
		called = true
		return nil, expectedErr
	}
	reg := NewRegistry()
	err := reg.Register(repoType, constructor)
	require.NoError(t, err)
	assert.Contains(t, reg.constructors, repoType)

	cfg := RepoConfig{Type: repoType}
	repo, err := reg.NewRepository(context.Background(), cfg)
	assert.ErrorIs(t, err, expectedErr)
	assert.Nil(t, repo)
	assert.True(t, called, "Constructor was not called")
}

func TestRegistry_NewRepository_UnsupportedRepoType(t *testing.T) {
	repoType := "repoType"
	cfg := RepoConfig{Type: repoType}
	reg := NewRegistry()
	repo, err := reg.NewRepository(context.Background(), cfg)
	assert.Error(t, err)
	assert.Nil(t, repo)
}

type dummyRepo struct{}

func (d dummyRepo) SampleTable(context.Context, *TableMetadata, SampleParameters) (
	Sample,
	error,
) {
	panic("not implemented")
}

func (d dummyRepo) ListDatabases(context.Context) ([]string, error) {
	panic("not implemented")
}

func (d dummyRepo) Introspect(context.Context) (*Metadata, error) {
	panic("not implemented")
}

func (d dummyRepo) Ping(context.Context) error {
	panic("not implemented")
}

func (d dummyRepo) Close() error {
	panic("not implemented")
}
