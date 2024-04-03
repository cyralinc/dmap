package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_sampleAllDbs_Error(t *testing.T) {
	ctx := context.Background()
	listDbErr := errors.New("error listing databases")
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(ctx).Return(nil, listDbErr)
	repo.EXPECT().Close().Return(nil)
	ctor := func(ctx context.Context, cfg RepoConfig) (Repository, error) {
		return repo, nil
	}
	cfg := RepoConfig{}
	samples, err := sampleAllDbs(ctx, ctor, cfg, IntrospectParameters{}, 0, 0)
	require.Nil(t, samples)
	require.ErrorIs(t, err, listDbErr)
}

func Test_sampleAllDbs_Successful_TwoDatabases(t *testing.T) {
	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	// Dummy metadata returned for each Introspect call
	meta := Metadata{
		Database: "db",
		Schemas: map[string]*SchemaMetadata{
			"schema": {
				Name: "schema",
				Tables: map[string]*TableMetadata{
					"table": {
						Schema: "schema",
						Name:   "table",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema",
								Table:    "table",
								Name:     "attr",
								DataType: "string",
							},
						},
					},
				},
			},
		},
	}
	sample := Sample{
		TablePath: []string{"db", "schema", "table"},
		Results: []SampleResult{
			{
				"attr": "foo",
			},
		},
	}
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(ctx).Return(dbs, nil)
	repo.EXPECT().Introspect(ctx, mock.Anything).Return(&meta, nil)
	repo.EXPECT().SampleTable(ctx, mock.Anything).Return(sample, nil)
	repo.EXPECT().Close().Return(nil)
	ctor := func(ctx context.Context, cfg RepoConfig) (Repository, error) {
		return repo, nil
	}
	samples, err := sampleAllDbs(ctx, ctor, RepoConfig{}, IntrospectParameters{}, 0, 0)
	require.NoError(t, err)
	// Two databases should be sampled, and our mock will return the sample for
	// each sample call. This really just asserts that we've sampled the correct
	// number of times.
	require.ElementsMatch(t, samples, []Sample{sample, sample})
}

func Test_sampleAllDbs_IntrospectError(t *testing.T) {
	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	introspectErr := errors.New("introspect error")
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(ctx).Return(dbs, nil)
	repo.EXPECT().Introspect(ctx, mock.Anything).Return(nil, introspectErr)
	repo.EXPECT().Close().Return(nil)
	ctor := func(ctx context.Context, cfg RepoConfig) (Repository, error) {
		return repo, nil
	}
	samples, err := sampleAllDbs(ctx, ctor, RepoConfig{}, IntrospectParameters{}, 0, 0)
	require.Empty(t, samples)
	require.NoError(t, err)
}

func Test_sampleAllDbs_SampleError(t *testing.T) {
	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	// Dummy metadata returned for each Introspect call
	meta := Metadata{
		Database: "db",
		Schemas: map[string]*SchemaMetadata{
			"schema": {
				Name: "schema",
				Tables: map[string]*TableMetadata{
					"table": {
						Schema: "schema",
						Name:   "table",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema",
								Table:    "table",
								Name:     "attr",
								DataType: "string",
							},
						},
					},
				},
			},
		},
	}
	sampleErr := errors.New("sample error")
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(ctx).Return(dbs, nil)
	repo.EXPECT().Introspect(ctx, mock.Anything).Return(&meta, nil)
	repo.EXPECT().SampleTable(ctx, mock.Anything).Return(Sample{}, sampleErr)
	repo.EXPECT().Close().Return(nil)
	ctor := func(ctx context.Context, cfg RepoConfig) (Repository, error) {
		return repo, nil
	}
	samples, err := sampleAllDbs(ctx, ctor, RepoConfig{}, IntrospectParameters{}, 0, 0)
	require.NoError(t, err)
	require.Empty(t, samples)
}

func Test_sampleAllDbs_TwoDatabases_OneSampleError(t *testing.T) {
	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	// Dummy metadata returned for each Introspect call
	meta := Metadata{
		Database: "db",
		Schemas: map[string]*SchemaMetadata{
			"schema": {
				Name: "schema",
				Tables: map[string]*TableMetadata{
					"table": {
						Schema: "schema",
						Name:   "table",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema",
								Table:    "table",
								Name:     "attr",
								DataType: "string",
							},
						},
					},
				},
			},
		},
	}
	sample := Sample{
		TablePath: []string{"db", "schema", "table"},
		Results: []SampleResult{
			{
				"attr": "foo",
			},
		},
	}
	sampleErr := errors.New("sample error")
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(ctx).Return(dbs, nil)
	repo.EXPECT().Introspect(ctx, mock.Anything).Return(&meta, nil)
	repo.EXPECT().SampleTable(ctx, mock.Anything).Return(sample, nil).Once()
	repo.EXPECT().SampleTable(ctx, mock.Anything).Return(Sample{}, sampleErr).Once()
	repo.EXPECT().Close().Return(nil)
	ctor := func(ctx context.Context, cfg RepoConfig) (Repository, error) {
		return repo, nil
	}
	samples, err := sampleAllDbs(ctx, ctor, RepoConfig{}, IntrospectParameters{}, 0, 0)
	require.NoError(t, err)
	// Because of a single sample error, we expect only one database was
	// sampled.
	require.ElementsMatch(t, samples, []Sample{sample})
}
