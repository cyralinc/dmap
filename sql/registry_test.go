package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistry_Register_Successful(t *testing.T) {
	repoType := "repoType"
	constructor := func(context.Context, RepoConfig) (Repository, error) {
		return nil, nil
	}
	reg := NewRegistry()
	err := reg.Register(repoType, constructor)
	require.NoError(t, err)
	require.Contains(t, reg.constructors, repoType)
}

func TestRegistry_MustRegister_NilConstructor(t *testing.T) {
	reg := NewRegistry()
	require.Panics(t, func() { reg.MustRegister("repoType", nil) })
}

func TestRegistry_MustRegister_TwoCalls_Panics(t *testing.T) {
	repoType := "repoType"
	constructor := func(context.Context, RepoConfig) (Repository, error) {
		return nil, nil
	}
	reg := NewRegistry()
	reg.MustRegister(repoType, constructor)
	require.Contains(t, reg.constructors, repoType)
	require.Panics(t, func() { reg.MustRegister(repoType, constructor) })
}

func TestRegistry_NewRepository_IsSuccessful(t *testing.T) {
	repoType := "repoType"
	called := false
	expectedRepo := (Repository)(nil)
	constructor := func(context.Context, RepoConfig) (Repository, error) {
		called = true
		return expectedRepo, nil
	}
	reg := NewRegistry()
	err := reg.Register(repoType, constructor)
	require.NoError(t, err)
	require.Contains(t, reg.constructors, repoType)

	repo, err := reg.NewRepository(context.Background(), repoType, RepoConfig{})
	require.NoError(t, err)
	require.Equal(t, expectedRepo, repo)
	require.True(t, called, "Constructor was not called")
}

func TestRegistry_NewRepository_ConstructorError(t *testing.T) {
	repoType := "repoType"
	called := false
	expectedErr := errors.New("dummy error")
	constructor := func(context.Context, RepoConfig) (Repository, error) {
		called = true
		return nil, expectedErr
	}
	reg := NewRegistry()
	err := reg.Register(repoType, constructor)
	require.NoError(t, err)
	require.Contains(t, reg.constructors, repoType)

	repo, err := reg.NewRepository(context.Background(), repoType, RepoConfig{})
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, repo)
	require.True(t, called, "Constructor was not called")
}

func TestRegistry_NewRepository_UnsupportedRepoType(t *testing.T) {
	repoType := "repoType"
	reg := NewRegistry()
	repo, err := reg.NewRepository(context.Background(), repoType, RepoConfig{})
	require.Error(t, err)
	require.Nil(t, repo)
}
