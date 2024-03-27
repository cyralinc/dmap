package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSampleRepository(t *testing.T) {
	ctx := context.Background()
	repo := fakeRepo{}
	params := SampleParameters{SampleSize: 2}
	samples, err := SampleRepository(ctx, repo, params)
	require.NoError(t, err)

	// Order is not important and is actually non-deterministic due to concurrency
	assert.ElementsMatch(t, samples, []Sample{table1Sample, table2Sample})
}

func TestSampleRepository_PartialError(t *testing.T) {
	ctx := context.Background()
	repo := fakeRepo{includeForbiddenTables: true}
	params := SampleParameters{SampleSize: 2}
	samples, err := SampleRepository(ctx, repo, params)
	require.ErrorContains(t, err, "forbidden table")

	// Order is not important and is actually non-deterministic due to concurrency
	assert.ElementsMatch(t, samples, []Sample{table1Sample, table2Sample})
}

var repoMeta = Metadata{
	Name:     "name",
	RepoType: "repoType",
	Database: "database",
	Schemas: map[string]*SchemaMetadata{
		"schema1": {
			Name: "",
			Tables: map[string]*TableMetadata{
				"table1": {
					Schema: "schema1",
					Name:   "table1",
					Attributes: []*AttributeMetadata{
						{
							Schema:   "schema1",
							Table:    "table1",
							Name:     "name1",
							DataType: "varchar",
						},
						{
							Schema:   "schema1",
							Table:    "table1",
							Name:     "name2",
							DataType: "decimal",
						},
					},
				},
			},
		},
		"schema2": {
			Name: "",
			Tables: map[string]*TableMetadata{
				"table2": {
					Schema: "schema2",
					Name:   "table2",
					Attributes: []*AttributeMetadata{
						{
							Schema:   "schema2",
							Table:    "table2",
							Name:     "name3",
							DataType: "int",
						},
						{
							Schema:   "schema2",
							Table:    "table2",
							Name:     "name4",
							DataType: "timestamp",
						},
					},
				},
			},
		},
	},
}

var schema1ForbiddenTable = TableMetadata{
	Schema: "schema1",
	Name:   "forbidden",
	Attributes: []*AttributeMetadata{
		{
			Schema:   "schema1",
			Table:    "forbidden",
			Name:     "name1",
			DataType: "varchar",
		},
		{
			Schema:   "schema1",
			Table:    "forbidden",
			Name:     "name2",
			DataType: "decimal",
		},
	},
}

var table1Sample = Sample{
	Metadata: SampleMetadata{
		Repo:     "name",
		Database: "database",
		Schema:   "schema1",
		Table:    "table1",
	},
	Results: []SampleResult{
		{
			"name1": "foo",
			"name2": "bar",
		},
		{
			"name1": "baz",
			"name2": "qux",
		},
	},
}

var table2Sample = Sample{
	Metadata: SampleMetadata{
		Repo:     "name",
		Database: "database",
		Schema:   "schema2",
		Table:    "table2",
	},
	Results: []SampleResult{
		{
			"name3": "foo1",
			"name4": "bar1",
		},
		{
			"name3": "baz1",
			"name4": "qux1",
		},
	},
}

type fakeRepo struct {
	includeForbiddenTables bool
}

func (f fakeRepo) Introspect(context.Context) (*Metadata, error) {
	if f.includeForbiddenTables {
		repoMeta.Schemas["schema1"].Tables["forbidden"] = &schema1ForbiddenTable
	}
	return &repoMeta, nil
}

func (f fakeRepo) SampleTable(_ context.Context, meta *TableMetadata, _ SampleParameters) (
	Sample,
	error,
) {
	if meta.Name == "table1" {
		return table1Sample, nil
	} else if meta.Name == "table2" {
		return table2Sample, nil
	} else if meta.Name == "forbidden" {
		return Sample{}, errors.New("forbidden table")
	} else {
		return Sample{}, errors.New("unrecognized table")
	}
}

func (f fakeRepo) ListDatabases(context.Context) ([]string, error) {
	panic("not implemented")
}

func (f fakeRepo) Ping(context.Context) error {
	panic("not implemented")
}

func (f fakeRepo) Close() error {
	panic("not implemented")
}
