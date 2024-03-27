package classification

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cyralinc/dmap/discovery/repository"
)

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

func Test_Classify_FakeClassifier_SingleSample(t *testing.T) {
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

	actual, err := ClassifySamples(context.Background(), classifier, []repository.Sample{sample})
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual)
}

func Test_AggregateClassify_FakeClassifier_MultipleSamples(t *testing.T) {
	tableName1 := tableName + "1"
	tableName2 := tableName + "2"
	ageAttr := "age"
	ssnAttr := "social_sec_num"
	ccnAttr := "credit_card_num"
	samples := []repository.Sample{
		{
			Metadata: repository.SampleMetadata{
				Repo:     repoName,
				Database: catalogName,
				Schema:   schemaName,
				Table:    tableName1,
			},
			Results: []repository.SampleResult{
				{
					ageAttr: "52",
					ssnAttr: "512-23-4256",
					ccnAttr: "4111111111111111",
				},
				{
					ageAttr: "53",
					ssnAttr: "512-23-4258",
					ccnAttr: "4111111111111111",
				},
			},
		},
		{
			Metadata: repository.SampleMetadata{
				Repo:     repoName,
				Database: catalogName,
				Schema:   schemaName,
				Table:    tableName2,
			},
			Results: []repository.SampleResult{
				{
					ageAttr: "21",
					ssnAttr: "123-45-6789",
					ccnAttr: "0123456789012345",
				},
				{
					ageAttr: "22",
					ssnAttr: "987-65-4321",
					ccnAttr: "0123456789012345",
				},
			},
		},
	}

	table1 := ClassifiedTable{
		Repo:    repoName,
		Catalog: catalogName,
		Schema:  schemaName,
		Table:   tableName1,
	}
	table2 := ClassifiedTable{
		Repo:    repoName,
		Catalog: catalogName,
		Schema:  schemaName,
		Table:   tableName2,
	}

	expectedFromClassifier1 := []Result{
		{
			Table:           &table1,
			AttributeName:   ageAttr,
			Classifications: []*Label{{Name: "PII"}},
		},
		{
			Table:           &table1,
			AttributeName:   ssnAttr,
			Classifications: []*Label{{Name: "PII"}, {Name: "PRIVATE"}},
		},
		{
			Table:           &table1,
			AttributeName:   ccnAttr,
			Classifications: []*Label{{Name: "PII"}, {Name: "CCN"}, {Name: "PCI"}},
		},
	}
	expectedFromClassifier2 := []Result{
		{
			Table:           &table2,
			AttributeName:   ageAttr,
			Classifications: []*Label{{Name: "PII"}},
		},
		{
			Table:           &table2,
			AttributeName:   ssnAttr,
			Classifications: []*Label{{Name: "PII"}, {Name: "PRIVATE"}},
		},
		{
			Table:           &table2,
			AttributeName:   ccnAttr,
			Classifications: []*Label{{Name: "PII"}, {Name: "CCN"}, {Name: "PCI"}},
		},
	}
	expected := append(expectedFromClassifier1, expectedFromClassifier2...)

	classifiers := map[string]Classifier{
		"classifier1": Classifier(
			fakeClassifier{
				classify: func(
					table *ClassifiedTable,
					attrs map[string]any,
				) ([]Result, error) {
					return expectedFromClassifier1, nil
				},
			},
		),
		"classifier2": Classifier(
			fakeClassifier{
				classify: func(
					table *ClassifiedTable,
					attrs map[string]any,
				) ([]Result, error) {
					return expectedFromClassifier2, nil
				},
			},
		),
	}

	actual, err := AggregateClassifySamples(context.Background(), classifiers, samples)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual)
}
