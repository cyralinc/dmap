package classification

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/discovery/repository"
)

func TestClassifySamples_SingleSample(t *testing.T) {
	repoName := "repoName"
	catalogName := "catalogName"
	schemaName := "schema"
	tableName := "table"

	sample := repository.Sample{
		Metadata: repository.SampleMetadata{
			Repo:     repoName,
			Database: catalogName,
			Schema:   schemaName,
			Table:    tableName,
		},
		Results: []repository.SampleResult{
			{
				"age":             "52",
				"social_sec_num":  "512-23-4256",
				"credit_card_num": "4111111111111111",
			},
			{
				"age":             "4111111111111111",
				"social_sec_num":  "512-23-4258",
				"credit_card_num": "4111111111111111",
			},
		},
	}

	classifiers := []Classifier{
		newTestLabelClassifier(t, "AGE"),
		newTestLabelClassifier(t, "CCN"),
	}

	actual, err := ClassifySamples(
		context.Background(),
		[]repository.Sample{sample},
		classifiers...,
	)
	require.NoError(t, err)

	table := &ClassifiedTable{
		Repo:    repoName,
		Catalog: catalogName,
		Schema:  schemaName,
		Table:   tableName,
	}

	expected := []Result{
		{
			Table:           table,
			AttributeName:   "age",
			Classifications: []*Label{{Name: "AGE"}},
		},
		{
			Table:           table,
			AttributeName:   "credit_card_num",
			Classifications: []*Label{{Name: "CCN"}},
		},
		{
			Table:           table,
			AttributeName:   "age",
			Classifications: []*Label{{Name: "CCN"}},
		},
	}

	require.Len(t, actual, len(expected))

	for i, got := range actual {
		want := expected[i]
		require.Equal(t, want.Table, got.Table)
		require.Equal(t, want.AttributeName, got.AttributeName)
		require.Len(t, got.Classifications, len(want.Classifications))
		for j, cl := range got.Classifications {
			wantCl := want.Classifications[j]
			require.Equal(t, wantCl.Name, cl.Name)
		}
	}
}

func TestClassifySamples_MultipleSamples(t *testing.T) {
	repoName := "repoName"
	catalogName := "catalogName"
	schemaName := "schema"
	tableName := "table"

	metadata1 := repository.SampleMetadata{
		Repo:     repoName,
		Database: catalogName,
		Schema:   schemaName,
		Table:    tableName,
	}

	metadata2 := repository.SampleMetadata{
		Repo:     repoName,
		Database: catalogName,
		Schema:   schemaName + "2",
		Table:    tableName + "2",
	}

	samples := []repository.Sample{
		{
			Metadata: metadata1,
			Results: []repository.SampleResult{
				{
					"age":             "52",
					"social_sec_num":  "512-23-4256",
					"credit_card_num": "4111111111111111",
				},
			},
		},
		{
			Metadata: metadata1,
			Results: []repository.SampleResult{
				{
					"age":             "52",
					"social_sec_num":  "512-23-4256",
					"credit_card_num": "4111111111111112",
				},
			},
		},
		{
			Metadata: metadata2,
			Results: []repository.SampleResult{
				{
					"age":             "52",
					"name":            "Joe Smith",
					"social_sec_num":  "512-23-4256",
					"credit_card_num": "4111111111111112",
				},
				{
					"age":             "4111111111111113",
					"name":            "Joe Smith",
					"social_sec_num":  "512-23-4256",
					"credit_card_num": "4111111111111112",
				},
			},
		},
	}

	classifiers := []Classifier{
		newTestLabelClassifier(t, "AGE"),
		newTestLabelClassifier(t, "CCN"),
	}

	actual, err := ClassifySamples(context.Background(), samples, classifiers...)
	require.NoError(t, err)

	table1 := &ClassifiedTable{
		Repo:    metadata1.Repo,
		Catalog: metadata1.Database,
		Schema:  metadata1.Schema,
		Table:   metadata1.Table,
	}

	table2 := &ClassifiedTable{
		Repo:    metadata2.Repo,
		Catalog: metadata2.Database,
		Schema:  metadata2.Schema,
		Table:   metadata2.Table,
	}

	expected := []Result{
		{
			Table:           table1,
			AttributeName:   "age",
			Classifications: []*Label{{Name: "AGE"}},
		},
		{
			Table:           table2,
			AttributeName:   "age",
			Classifications: []*Label{{Name: "AGE"}},
		},
		{
			Table:           table1,
			AttributeName:   "credit_card_num",
			Classifications: []*Label{{Name: "CCN"}},
		},
		{
			Table:           table2,
			AttributeName:   "credit_card_num",
			Classifications: []*Label{{Name: "CCN"}},
		},
		{
			Table:           table2,
			AttributeName:   "age",
			Classifications: []*Label{{Name: "CCN"}},
		},
	}
	require.Len(t, actual, len(expected))

	for i, got := range actual {
		want := expected[i]
		require.Equal(t, want.Table, got.Table)
		require.Equal(t, want.AttributeName, got.AttributeName)
		require.Len(t, got.Classifications, len(want.Classifications))
		for j, cl := range got.Classifications {
			wantCl := want.Classifications[j]
			require.Equal(t, wantCl.Name, cl.Name)
		}
	}
}

type classifyFunc func(table *ClassifiedTable, attrs map[string]any) ([]Result, error)

type fakeClassifier struct {
	classify classifyFunc
}

var _ Classifier = (*fakeClassifier)(nil)

func (f fakeClassifier) Classify(
	_ context.Context,
	table *ClassifiedTable,
	attrs map[string]any,
) ([]Result, error) {
	return f.classify(table, attrs)
}

func TestClassifySamples_FakeClassifier_SingleSample(t *testing.T) {
	repoName := "repoName"
	catalogName := "catalogName"
	schemaName := "schema"
	tableName := "table"

	sample := repository.Sample{
		Metadata: repository.SampleMetadata{
			Repo:     repoName,
			Database: catalogName,
			Schema:   schemaName,
			Table:    tableName,
		},
		Results: []repository.SampleResult{
			{
				"age":             "52",
				"social_sec_num":  "512-23-4256",
				"credit_card_num": "4111111111111111",
			},
			{
				"age":             "53",
				"social_sec_num":  "512-23-4258",
				"credit_card_num": "4111111111111111",
			},
		},
	}

	table := ClassifiedTable{
		Repo:    repoName,
		Catalog: catalogName,
		Schema:  schemaName,
		Table:   tableName,
	}

	expected := []Result{
		{
			Table:           &table,
			AttributeName:   "age",
			Classifications: []*Label{{Name: "PII"}},
		},
		{
			Table:           &table,
			AttributeName:   "social_sec_num",
			Classifications: []*Label{{Name: "PII"}, {Name: "PRIVATE"}},
		},
		{
			Table:           &table,
			AttributeName:   "credit_card_num",
			Classifications: []*Label{{Name: "PII"}, {Name: "CCN"}, {Name: "PCI"}},
		},
	}

	classifier := fakeClassifier{
		classify: func(
			table *ClassifiedTable,
			attrs map[string]any,
		) ([]Result, error) {
			return expected, nil
		},
	}

	actual, err := ClassifySamples(
		context.Background(),
		[]repository.Sample{sample},
		classifier,
	)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual)
}

func newTestLabelClassifier(t *testing.T, lblName string) Classifier {
	fname := fmt.Sprintf("./rego/%s.rego", strings.ToLower(lblName))
	fin, err := os.ReadFile(fname)
	require.NoError(t, err)
	classifierCode := string(fin)
	lbl := Label{
		Name:               lblName,
		ClassificationRule: classifierCode,
	}
	classifier, err := NewLabelClassifier(&lbl)
	require.NoError(t, err)
	return classifier
}
