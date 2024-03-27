package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/discovery/config"
)

const mockRepoType = "mockRepo"

func setup(t *testing.T) *MockRepository {
	repo := NewMockRepository(t)
	Register(
		mockRepoType,
		func(ctx context.Context, cfg config.RepoConfig) (Repository, error) {
			return repo, nil
		},
	)
	return repo
}

func cleanup() {
	delete(registry, mockRepoType)
}

func TestSampleAllDatabases_Error(t *testing.T) {
	repo := setup(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	listDbErr := errors.New("error listing databases")
	repo.On("ListDatabases", ctx).Return(nil, listDbErr)
	cfg := config.RepoConfig{Type: mockRepoType}
	sampleParams := SampleParameters{SampleSize: 5}
	samples, err := SampleAllDatabases(ctx, repo, cfg, sampleParams)
	require.Nil(t, samples)
	require.ErrorIs(t, err, listDbErr)
}

func TestSampleAllDatabases_Successful_TwoDatabases(t *testing.T) {
	repo := setup(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	repo.On("ListDatabases", ctx).Return(dbs, nil)
	// Dummy metadata returned for each Introspect call
	meta := Metadata{
		Name:     "test",
		RepoType: mockRepoType,
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
	repo.On("Introspect", mock.Anything).Return(&meta, nil)
	sample := Sample{
		Metadata: SampleMetadata{
			Repo:     "repo",
			Database: "db",
			Schema:   "schema",
			Table:    "table",
		},
		Results: []SampleResult{
			{
				"attr": "foo",
			},
		},
	}
	repo.On("SampleTable", mock.Anything, mock.Anything, mock.Anything).
		Return(sample, nil)
	repo.On("Close").Return(nil)

	cfg := config.RepoConfig{Type: mockRepoType}
	sampleParams := SampleParameters{SampleSize: 5}
	samples, err := SampleAllDatabases(ctx, repo, cfg, sampleParams)
	require.NoError(t, err)
	// Two databases should be sampled, and our mock will return the sample for
	// each sample call. This really just asserts that we've sampled the correct
	// number of times.
	require.ElementsMatch(t, samples, []Sample{sample, sample})
}

func TestSampleAllDatabases_IntrospectError(t *testing.T) {
	repo := setup(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	repo.On("ListDatabases", ctx).Return(dbs, nil)
	introspectErr := errors.New("introspect error")
	repo.On("Introspect", mock.Anything).Return(nil, introspectErr)
	repo.On("Close").Return(nil)

	cfg := config.RepoConfig{Type: mockRepoType}
	sampleParams := SampleParameters{SampleSize: 5}
	samples, err := SampleAllDatabases(ctx, repo, cfg, sampleParams)
	require.Empty(t, samples)
	require.NoError(t, err)
}

func TestSampleAllDatabases_SampleError(t *testing.T) {
	repo := setup(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	repo.On("ListDatabases", ctx).Return(dbs, nil)
	// Dummy metadata returned for each Introspect call
	meta := Metadata{
		Name:     "test",
		RepoType: mockRepoType,
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
	repo.On("Introspect", mock.Anything).Return(&meta, nil)
	sampleErr := errors.New("sample error")
	repo.On("SampleTable", mock.Anything, mock.Anything, mock.Anything).
		Return(Sample{}, sampleErr)
	repo.On("Close").Return(nil)

	cfg := config.RepoConfig{Type: mockRepoType}
	sampleParams := SampleParameters{SampleSize: 5}
	samples, err := SampleAllDatabases(ctx, repo, cfg, sampleParams)
	require.NoError(t, err)
	require.Empty(t, samples)
}

func TestSampleAllDatabases_TwoDatabases_OneSampleError(t *testing.T) {
	repo := setup(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	repo.On("ListDatabases", ctx).Return(dbs, nil)
	// Dummy metadata returned for each Introspect call
	meta := Metadata{
		Name:     "test",
		RepoType: mockRepoType,
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
	repo.On("Introspect", mock.Anything).Return(&meta, nil)
	sample := Sample{
		Metadata: SampleMetadata{
			Repo:     "repo",
			Database: "db",
			Schema:   "schema",
			Table:    "table",
		},
		Results: []SampleResult{
			{
				"attr": "foo",
			},
		},
	}
	repo.On("SampleTable", mock.Anything, mock.Anything, mock.Anything).
		Return(sample, nil).Once()
	sampleErr := errors.New("sample error")
	repo.On("SampleTable", mock.Anything, mock.Anything, mock.Anything).
		Return(Sample{}, sampleErr).Once()
	repo.On("Close").Return(nil)

	cfg := config.RepoConfig{Type: mockRepoType}
	sampleParams := SampleParameters{SampleSize: 5}
	samples, err := SampleAllDatabases(ctx, repo, cfg, sampleParams)
	require.NoError(t, err)
	// Because of a single sample error, we expect only one database was
	// sampled.
	require.ElementsMatch(t, samples, []Sample{sample})
}
