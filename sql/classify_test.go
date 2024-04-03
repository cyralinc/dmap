package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/classification"
	"github.com/cyralinc/dmap/scan"
)

func Test_classifySamples_SingleSample(t *testing.T) {
	ctx := context.Background()
	sample := Sample{
		TablePath: []string{"db", "schema", "table"},
		Results: []SampleResult{
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
		classification.Result{
			"age":             lblSet("AGE"),
			"social_sec_num":  lblSet("SSN"),
			"credit_card_num": lblSet("CCN"),
		},
		nil,
	)
	classifier.EXPECT().Classify(ctx, map[string]any(sample.Results[1])).Return(
		classification.Result{
			"age":             lblSet("AGE", "CVV"),
			"credit_card_num": lblSet("CCN"),
		},
		nil,
	)

	expected := []scan.Classification{
		{
			AttributePath: append(sample.TablePath, "age"),
			Labels:        lblSet("AGE", "CVV"),
		},
		{
			AttributePath: append(sample.TablePath, "social_sec_num"),
			Labels:        lblSet("SSN"),
		},
		{
			AttributePath: append(sample.TablePath, "credit_card_num"),
			Labels:        lblSet("CCN"),
		},
	}
	actual, err := classifySamples(ctx, []Sample{sample}, classifier)
	require.NoError(t, err)
	require.ElementsMatch(t, expected, actual)
}

func Test_classifySamples_MultipleSamples(t *testing.T) {
	ctx := context.Background()
	samples := []Sample{
		{
			TablePath: []string{"db1", "schema1", "table1"},
			Results: []SampleResult{
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
			TablePath: []string{"db2", "schema2", "table2"},
			Results: []SampleResult{
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
		classification.Result{
			"age":             lblSet("AGE"),
			"social_sec_num":  lblSet("SSN"),
			"credit_card_num": lblSet("CCN"),
		},
		nil,
	)
	classifier.EXPECT().Classify(ctx, map[string]any(samples[0].Results[1])).Return(
		classification.Result{
			"age":             lblSet("AGE", "CVV"),
			"credit_card_num": lblSet("CCN"),
		},
		nil,
	)
	classifier.EXPECT().Classify(ctx, map[string]any(samples[1].Results[0])).Return(
		classification.Result{
			"fullname": lblSet("FULL_NAME"),
			"dob":      lblSet("DOB"),
		},
		nil,
	)

	expected := []scan.Classification{
		{
			AttributePath: append(samples[0].TablePath, "age"),
			Labels:        lblSet("AGE", "CVV"),
		},
		{
			AttributePath: append(samples[0].TablePath, "social_sec_num"),
			Labels:        lblSet("SSN"),
		},
		{
			AttributePath: append(samples[0].TablePath, "credit_card_num"),
			Labels:        lblSet("CCN"),
		},
		{
			AttributePath: append(samples[1].TablePath, "fullname"),
			Labels:        lblSet("FULL_NAME"),
		},
		{
			AttributePath: append(samples[1].TablePath, "dob"),
			Labels:        lblSet("DOB"),
		},
	}
	actual, err := classifySamples(ctx, samples, classifier)
	require.NoError(t, err)
	require.ElementsMatch(t, expected, actual)
}

func lblSet(labels ...string) classification.LabelSet {
	set := make(classification.LabelSet)
	for _, label := range labels {
		set[label] = struct {
		}{}
	}
	return set
}
