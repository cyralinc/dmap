package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/discovery/config"
)

func TestRegister_Successful(t *testing.T) {
	require.Empty(t, registry)

	repoType := "repoType"
	constructor := func(context.Context, config.RepoConfig) (Repository, error) {
		return nil, nil
	}

	Register(repoType, constructor)
	assert.Contains(t, registry, repoType)

	t.Cleanup(func() { delete(registry, repoType) })
}

func TestRegister_NilConstructor(t *testing.T) {
	require.Empty(t, registry)
	assert.Panics(t, func() { Register("repoType", nil) })
	assert.Empty(t, registry)
}

func TestRegister_TwoCalls_Panics(t *testing.T) {
	require.Empty(t, registry)

	repoType := "repoType"
	constructor := func(context.Context, config.RepoConfig) (Repository, error) {
		return nil, nil
	}

	Register(repoType, constructor)
	assert.Contains(t, registry, repoType)

	assert.Panics(t, func() { Register(repoType, constructor) })

	t.Cleanup(func() { delete(registry, repoType) })
}

func TestNewRepository_IsSuccessful(t *testing.T) {
	require.Empty(t, registry)

	repoType := "repoType"

	called := false
	expectedRepo := dummyRepo{}
	constructor := func(context.Context, config.RepoConfig) (Repository, error) {
		called = true
		return expectedRepo, nil
	}

	Register(repoType, constructor)
	assert.Contains(t, registry, repoType)

	cfg := config.RepoConfig{Type: repoType}
	repo, err := NewRepository(context.Background(), cfg)
	assert.NoError(t, err)
	assert.Equal(t, expectedRepo, repo)
	assert.True(t, called, "Constructor was not called")

	t.Cleanup(func() { delete(registry, repoType) })
}

func TestNewRepository_ConstructorError(t *testing.T) {
	require.Empty(t, registry)

	repoType := "repoType"

	called := false
	expectedErr := errors.New("dummy error")
	constructor := func(context.Context, config.RepoConfig) (Repository, error) {
		called = true
		return nil, expectedErr
	}

	Register(repoType, constructor)
	assert.Contains(t, registry, repoType)

	cfg := config.RepoConfig{Type: repoType}
	repo, err := NewRepository(context.Background(), cfg)
	assert.ErrorIs(t, err, expectedErr)
	assert.Nil(t, repo)
	assert.True(t, called, "Constructor was not called")

	t.Cleanup(func() { delete(registry, repoType) })
}

func TestNewRepository_UnsupportedRepoType(t *testing.T) {
	require.Empty(t, registry)

	repoType := "repoType"

	cfg := config.RepoConfig{Type: repoType}
	repo, err := NewRepository(context.Background(), cfg)
	assert.Error(t, err)
	assert.Nil(t, repo)

	t.Cleanup(func() { delete(registry, repoType) })
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
