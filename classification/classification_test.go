package classification

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/discovery/repository"
)

func TestMerge_WhenCalledOnNilReceiver_ShouldNotPanic(t *testing.T) {
	var result Result
	require.NotPanics(
		t, func() {
			result.Merge(Result{"age": {"AGE": {Name: "AGE"}}})
		},
	)
}

func TestMerge_WhenCalledWithNonExistingAttributes_ShouldAddThem(t *testing.T) {
	result := Result{
		"age": {"AGE": {Name: "AGE"}},
	}
	other := Result{
		"social_sec_num": {"SSN": {Name: "SSN"}},
	}
	expected := Result{
		"age":            {"AGE": {Name: "AGE"}},
		"social_sec_num": {"SSN": {Name: "SSN"}},
	}
	result.Merge(other)
	require.Equal(t, expected, result)
}

func TestMerge_WhenCalledWithExistingAttributes_ShouldMergeLabelSets(t *testing.T) {
	result := Result{
		"age": {"AGE": {Name: "AGE"}},
	}
	other := Result{
		"age": {"CVV": {Name: "CVV"}},
	}
	expected := Result{
		"age": {"AGE": {Name: "AGE"}, "CVV": {Name: "CVV"}},
	}
	result.Merge(other)
	require.Equal(t, expected, result)
}

func TestMerge_WhenCalledWithExistingAttributes_ShouldOverwrite(t *testing.T) {
	result := Result{
		"age": {"AGE": {Name: "AGE", Description: "Foo"}},
	}
	other := Result{
		"age": {"AGE": {Name: "AGE", Description: "Bar"}},
	}
	expected := Result{
		"age": {"AGE": {Name: "AGE", Description: "Bar"}},
	}
	result.Merge(other)
	require.Equal(t, expected, result)
}

func TestClassifySamples_SingleTable(t *testing.T) {
	ctx := context.Background()
	meta := sql.SampleMetadata{
		Repo:     "repo",
		Database: "db",
		Schema:   "schema",
		Table:    "table",
	}

	sample := sql.Sample{
		Metadata: meta,
		Results: []sql.SampleResult{
			{
				"age":             "52",
				"social_sec_num":  "512-23-4258",
				"credit_card_num": "4111111111111111",
			},
			{
				"age":             "101",
				"social_sec_num":  "foobarbaz",
				"credit_card_num": "4111111111111111",
			},
		},
	}

	classifier := NewMockClassifier(t)
	// Need to explicitly convert it to a map because Mockery isn't smart enough
	// to infer the type.
	classifier.EXPECT().Classify(ctx, map[string]any(sample.Results[0])).Return(
		Result{
			"age": {
				"AGE": {Name: "AGE"},
			},
			"social_sec_num": {
				"SSN": {Name: "SSN"},
			},
			"credit_card_num": {
				"CCN": {Name: "CCN"},
			},
		},
		nil,
	)
	classifier.EXPECT().Classify(ctx, map[string]any(sample.Results[1])).Return(
		Result{
			"age": {
				"AGE": {Name: "AGE"},
				"CVV": {Name: "CVV"},
			},
			"credit_card_num": {
				"CCN": {Name: "CCN"},
			},
		},
		nil,
	)

	expected := []ClassifiedTable{
		{
			Repo:     meta.Repo,
			Database: meta.Database,
			Schema:   meta.Schema,
			Table:    meta.Table,
			Classifications: Result{
				"age": {
					"AGE": {Name: "AGE"},
					"CVV": {Name: "CVV"},
				},
				"social_sec_num": {
					"SSN": {Name: "SSN"},
				},
				"credit_card_num": {
					"CCN": {Name: "CCN"},
				},
			},
		},
	}
	actual, err := ClassifySamples(ctx, []sql.Sample{sample}, classifier)
	require.NoError(t, err)
	require.Len(t, actual, len(expected))
	for i := range actual {
		requireClassifiedTableEqual(t, expected[i], actual[i])
	}
}

func TestClassifySamples_MultipleTables(t *testing.T) {
	ctx := context.Background()
	meta1 := sql.SampleMetadata{
		Repo:     "repo1",
		Database: "db1",
		Schema:   "schema1",
		Table:    "table1",
	}
	meta2 := sql.SampleMetadata{
		Repo:     "repo2",
		Database: "db2",
		Schema:   "schema2",
		Table:    "table2",
	}

	samples := []sql.Sample{
		{
			Metadata: meta1,
			Results: []sql.SampleResult{
				{
					"age":             "52",
					"social_sec_num":  "512-23-4258",
					"credit_card_num": "4111111111111111",
				},
				{
					"age":             "101",
					"social_sec_num":  "foobarbaz",
					"credit_card_num": "4111111111111111",
				},
			},
		},
		{
			Metadata: meta2,
			Results: []sql.SampleResult{
				{
					"fullname": "John Doe",
					"dob":      "2000-01-01",
					"random":   "foobarbaz",
				},
			},
		},
	}

	classifier := NewMockClassifier(t)
	// Need to explicitly convert it to a map because Mockery isn't smart enough
	// to infer the type.
	classifier.EXPECT().Classify(ctx, map[string]any(samples[0].Results[0])).Return(
		Result{
			"age": {
				"AGE": {Name: "AGE"},
			},
			"social_sec_num": {
				"SSN": {Name: "SSN"},
			},
			"credit_card_num": {
				"CCN": {Name: "CCN"},
			},
		},
		nil,
	)
	classifier.EXPECT().Classify(ctx, map[string]any(samples[0].Results[1])).Return(
		Result{
			"age": {
				"AGE": {Name: "AGE"},
				"CVV": {Name: "CVV"},
			},
			"credit_card_num": {
				"CCN": {Name: "CCN"},
			},
		},
		nil,
	)
	classifier.EXPECT().Classify(ctx, map[string]any(samples[1].Results[0])).Return(
		Result{
			"fullname": {
				"FULL_NAME": {Name: "FULL_NAME"},
			},
			"dob": {
				"DOB": {Name: "DOB"},
			},
		},
		nil,
	)

	expected := []ClassifiedTable{
		{
			Repo:     meta1.Repo,
			Database: meta1.Database,
			Schema:   meta1.Schema,
			Table:    meta1.Table,
			Classifications: Result{
				"age": {
					"AGE": {Name: "AGE"},
					"CVV": {Name: "CVV"},
				},
				"social_sec_num": {
					"SSN": {Name: "SSN"},
				},
				"credit_card_num": {
					"CCN": {Name: "CCN"},
				},
			},
		},
		{
			Repo:     meta2.Repo,
			Database: meta2.Database,
			Schema:   meta2.Schema,
			Table:    meta2.Table,
			Classifications: Result{
				"fullname": {
					"FULL_NAME": {Name: "FULL_NAME"},
				},
				"dob": {
					"DOB": {Name: "DOB"},
				},
			},
		},
	}
	actual, err := ClassifySamples(ctx, samples, classifier)
	require.NoError(t, err)
	require.Len(t, actual, len(expected))
	for i := range actual {
		requireClassifiedTableEqual(t, expected[i], actual[i])
	}
}

func requireClassifiedTableEqual(t *testing.T, expected, actual ClassifiedTable) {
	require.Equal(t, expected.Repo, actual.Repo)
	require.Equal(t, expected.Database, actual.Database)
	require.Equal(t, expected.Schema, actual.Schema)
	require.Equal(t, expected.Table, actual.Table)
	requireResultEqual(t, expected.Classifications, actual.Classifications)
}
