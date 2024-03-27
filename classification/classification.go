// Package classification provides various types and functions to facilitate
// data classification. The type Classifier provides an interface which takes
// sampled data as input and returns a classified version of that sample as
// output. The package contains at least one implementation which uses Rego and
// OPA to perform the actual classification logic (see LabelClassifier), however
// other implementations may be added in the future.
package classification

import (
	"context"
	"fmt"

	"github.com/cyralinc/dmap/discovery/repository"
)

// TODO: godoc -ccampo 2024-03-27
type ClassifiedTable struct {
	Repo    string `json:"repo"`
	Catalog string `json:"catalog"`
	Schema  string `json:"schema"`
	Table   string `json:"table"`
}

// Result represents the classification of a data attribute.
type Result struct {
	Table           *ClassifiedTable `json:"table"`
	AttributeName   string           `json:"attributeName"`
	Classifications []*Label         `json:"classifications"`
}

// Classifier implementations know how to turn a row of data into a sequence of
// classification results.
type Classifier interface {
	// Classify takes as input what amounts to a "row of data": complete
	// information about where the table comes from as well as a list of columns
	// and attributeValues. While the values might be any data type, by the time
	// we reach here, we expect the values to be represented as strings.
	//
	// For a given attribute, if it is classified as belonging to a particular
	// classification group, we will add an instance for it in the Result.
	// If however, there is no assigned classification, we will skip it in the
	// results. A zero length return value is normal if none of the attributes
	// matched the classification requirements.
	Classify(
		ctx context.Context,
		table *ClassifiedTable,
		attrs map[string]any,
	) ([]Result, error)
}

// ClassifySamples uses the provided Classifier to classify the sample data
// passed via the "samples" parameter. It is mostly a helper function which
// loops through each repository.Sample, retrieves the attribute names and
// values of that sample, passes them to Classifier.Classify, and then
// aggregates the results. Please see the documentation for Classifier and its
// Classify method for more details. The returned slice represents all the
// unique classification results for a given sample set.
func ClassifySamples(
	ctx context.Context,
	classifier Classifier,
	samples []repository.Sample,
) ([]Result, error) {
	var classifications []Result
	for _, sample := range samples {
		table := ClassifiedTable{
			Repo:    sample.Metadata.Repo,
			Catalog: sample.Metadata.Database,
			Schema:  sample.Metadata.Schema,
			Table:   sample.Metadata.Table,
		}
		// Classify each sampled row
		for _, sampleResult := range sample.Results {
			res, err := classifier.Classify(ctx, &table, sampleResult)
			if err != nil {
				return nil, fmt.Errorf("error classifying sample: %w", err)
			}
			classifications = append(classifications, res...)
		}
	}
	return combineAndDedupe(classifications), nil
}

// AggregateClassifySamples classifies the given samples with every classifier,
// and returns the aggregate result slice. For details on how each
// classification is executed, see ClassifySamples.
func AggregateClassifySamples(
	ctx context.Context,
	classifiers map[string]Classifier,
	samples []repository.Sample,
) ([]Result, error) {
	classifications := make([]Result, 0)
	for _, classifier := range classifiers {
		classified, err := ClassifySamples(ctx, classifier, samples)
		if err != nil {
			return nil, err
		}
		classifications = append(classifications, classified...)
	}
	return classifications, nil
}

// combineAndDedupe takes a slice of Result and combines the individual elements
// when they have the same schema/table/attribute, but different labels, into a
// Result element with combined labels. Additionally, only distinct results by
// schema, table, and attribute are present in the return slice.
func combineAndDedupe(results []Result) []Result {
	set := make(map[tableAttrLabel]bool)
	distinctLabels := make(map[tableAttr][]*Label)
	for _, result := range results {
		for _, lbl := range result.Classifications {
			key := tableAttrLabel{
				ta: tableAttr{
					table: *result.Table,
					attr:  result.AttributeName,
				},
				label: lbl.Name,
			}

			if !set[key] {
				set[key] = true
				distinctLabels[key.ta] = append(distinctLabels[key.ta], lbl)
			}
		}
	}

	distinctResults := make([]Result, 0, len(distinctLabels))
	for ta, labels := range distinctLabels {
		result := Result{
			Table: &ClassifiedTable{
				Repo:    ta.table.Repo,
				Catalog: ta.table.Catalog,
				Schema:  ta.table.Schema,
				Table:   ta.table.Table,
			},
			AttributeName:   ta.attr,
			Classifications: labels,
		}
		distinctResults = append(distinctResults, result)
	}

	return distinctResults
}

// Both tableAttr and tableAttrLabel are only used as map keys
type tableAttr struct {
	table ClassifiedTable
	attr  string
}

type tableAttrLabel struct {
	ta    tableAttr
	label string
}
