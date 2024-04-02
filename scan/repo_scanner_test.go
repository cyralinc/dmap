package scan

import (
	"context"
	"testing"

	"github.com/cyralinc/dmap/classification"
	"github.com/cyralinc/dmap/discovery"
	"github.com/cyralinc/dmap/scan/mocks"

	"github.com/stretchr/testify/require"
)

func Test_classifySamples_SingleTable(t *testing.T) {
	ctx := context.Background()
	meta := discovery.SampleMetadata{
		Repo:     "repo",
		Database: "db",
		Schema:   "schema",
		Table:    "table",
	}

	sample := discovery.Sample{
		Metadata: meta,
		Results: []discovery.SampleResult{
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

	classifier := mocks.NewClassifier(t)
	// Need to explicitly convert it to a map because Mockery isn't smart enough
	// to infer the type.
	classifier.EXPECT().Classify(ctx, map[string]any(sample.Results[0])).Return(
		classification.Result{
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
		classification.Result{
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

	expected := []classification.ClassifiedTable{
		{
			Repo:     meta.Repo,
			Database: meta.Database,
			Schema:   meta.Schema,
			Table:    meta.Table,
			Classifications: classification.Result{
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
	actual, err := classifySamples(ctx, []discovery.Sample{sample}, classifier)
	require.NoError(t, err)
	require.Len(t, actual, len(expected))
	for i := range actual {
		requireClassifiedTableEqual(t, expected[i], actual[i])
	}
}

func Test_classifySamples_MultipleTables(t *testing.T) {
	ctx := context.Background()
	meta1 := discovery.SampleMetadata{
		Repo:     "repo1",
		Database: "db1",
		Schema:   "schema1",
		Table:    "table1",
	}
	meta2 := discovery.SampleMetadata{
		Repo:     "repo2",
		Database: "db2",
		Schema:   "schema2",
		Table:    "table2",
	}

	samples := []discovery.Sample{
		{
			Metadata: meta1,
			Results: []discovery.SampleResult{
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
			Results: []discovery.SampleResult{
				{
					"fullname": "John Doe",
					"dob":      "2000-01-01",
					"random":   "foobarbaz",
				},
			},
		},
	}

	classifier := mocks.NewClassifier(t)
	// Need to explicitly convert it to a map because Mockery isn't smart enough
	// to infer the type.
	classifier.EXPECT().Classify(ctx, map[string]any(samples[0].Results[0])).Return(
		classification.Result{
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
		classification.Result{
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
		classification.Result{
			"fullname": {
				"FULL_NAME": {Name: "FULL_NAME"},
			},
			"dob": {
				"DOB": {Name: "DOB"},
			},
		},
		nil,
	)

	expected := []classification.ClassifiedTable{
		{
			Repo:     meta1.Repo,
			Database: meta1.Database,
			Schema:   meta1.Schema,
			Table:    meta1.Table,
			Classifications: classification.Result{
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
			Classifications: classification.Result{
				"fullname": {
					"FULL_NAME": {Name: "FULL_NAME"},
				},
				"dob": {
					"DOB": {Name: "DOB"},
				},
			},
		},
	}
	actual, err := classifySamples(ctx, samples, classifier)
	require.NoError(t, err)
	require.Len(t, actual, len(expected))
	for i := range actual {
		requireClassifiedTableEqual(t, expected[i], actual[i])
	}
}

func requireClassifiedTableEqual(t *testing.T, expected, actual classification.ClassifiedTable) {
	require.Equal(t, expected.Repo, actual.Repo)
	require.Equal(t, expected.Database, actual.Database)
	require.Equal(t, expected.Schema, actual.Schema)
	require.Equal(t, expected.Table, actual.Table)
	requireResultEqual(t, expected.Classifications, actual.Classifications)
}

func requireResultEqual(t *testing.T, want, got classification.Result) {
	require.Len(t, got, len(want))
	for k, v := range want {
		gotSet, ok := got[k]
		require.Truef(t, ok, "missing attribute %s", k)
		requireLabelSetEqual(t, v, gotSet)
	}
}

func requireLabelSetEqual(t *testing.T, want, got classification.LabelSet) {
	require.Len(t, got, len(want))
	for k, v := range want {
		gotLbl, ok := got[k]
		require.Truef(t, ok, "missing label %s", k)
		require.Equal(t, v.Name, gotLbl.Name)
	}
}
