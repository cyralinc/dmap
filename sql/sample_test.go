package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	table1Sample = Sample{
		TablePath: []string{"database", "schema1", "table1"},
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

	table2Sample = Sample{
		TablePath: []string{"database", "schema2", "table2"},
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
)

func Test_sampleDb_Success(t *testing.T) {
	ctx := context.Background()
	repo := NewMockRepository(t)
	meta := Metadata{
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
	repo.EXPECT().Introspect(ctx, mock.Anything).Return(&meta, nil)
	sampleParams1 := SampleParameters{
		Metadata: meta.Schemas["schema1"].Tables["table1"],
	}
	sampleParams2 := SampleParameters{
		Metadata: meta.Schemas["schema2"].Tables["table2"],
	}
	repo.EXPECT().SampleTable(ctx, sampleParams1).Return(table1Sample, nil)
	repo.EXPECT().SampleTable(ctx, sampleParams2).Return(table2Sample, nil)
	samples, err := sampleDb(ctx, repo, IntrospectParameters{}, 0, 0)
	require.NoError(t, err)
	// Order is not important and is actually non-deterministic due to concurrency
	expected := []Sample{table1Sample, table2Sample}
	require.ElementsMatch(t, expected, samples)
}

func Test_sampleDb_PartialError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := NewMockRepository(t)
	meta := Metadata{
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
					"forbidden": {
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
	repo.EXPECT().Introspect(ctx, mock.Anything).Return(&meta, nil)
	sampleParams1 := SampleParameters{
		Metadata: meta.Schemas["schema1"].Tables["table1"],
	}
	sampleParams2 := SampleParameters{
		Metadata: meta.Schemas["schema1"].Tables["forbidden"],
	}
	sampleParamsForbidden := SampleParameters{
		Metadata: meta.Schemas["schema2"].Tables["table2"],
	}
	repo.EXPECT().SampleTable(ctx, sampleParams1).Return(table1Sample, nil)
	errForbidden := errors.New("forbidden table")
	repo.EXPECT().SampleTable(ctx, sampleParamsForbidden).Return(Sample{}, errForbidden)
	repo.EXPECT().SampleTable(ctx, sampleParams2).Return(table2Sample, nil)

	samples, err := sampleDb(ctx, repo, IntrospectParameters{}, 0, 0)
	require.ErrorIs(t, err, errForbidden)
	// Order is not important and is actually non-deterministic due to concurrency
	expected := []Sample{table1Sample, table2Sample}
	require.ElementsMatch(t, expected, samples)
}
