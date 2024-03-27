// Package classification provides various types and functions to facilitate
// data classification. The type Classifier provides an interface which takes
// arbitrary data as input and returns a classified version of that data as
// output. The package contains at least one implementation which uses OPA and
// Rego to perform the actual classification logic (see LabelClassifier),
// however other implementations may be added in the future.
package classification

import (
	"context"
	"fmt"
	"maps"

	"github.com/cyralinc/dmap/discovery/repository"
)

// Classifier is an interface that represents a data classifier. A classifier
// takes a set of data attributes and classifies them into a set of labels.
type Classifier interface {
	// Classify takes the given input, which amounts to essentially a "row of
	// data", and returns the data classifications for that input. The input is
	// a map of attribute names (i.e. columns) to their values. The returned
	// Result is a map of attribute names to the set of labels that attributes
	// were classified as.
	Classify(ctx context.Context, input map[string]any) (Result, error)
}

// ClassifiedTable represents a database table that has been classified. The
// classifications are stored in the Classifications field, which is a map of
// attribute names (i.e. columns) to the set of labels that attributes were
// classified as.
type ClassifiedTable struct {
	Repo            string `json:"repo"`
	Database        string `json:"database"`
	Schema          string `json:"schema"`
	Table           string `json:"table"`
	Classifications Result `json:"classifications"`
}

// Result represents the classifications for a set of data attributes. The key
// is the attribute (i.e. column) name and the value is the set of labels
// that attribute was classified as.
type Result map[string]LabelSet

// Merge combines the given other Result into this Result (the receiver). If
// an attribute from other is already present in this Result, the existing
// labels for that attribute are merged with the labels from other, otherwise
// labels from other for the attribute are simply added to this Result.
func (c Result) Merge(other Result) {
	if c == nil {
		return
	}
	for attr, labelSet := range other {
		if _, ok := c[attr]; !ok {
			c[attr] = make(LabelSet)
		}
		maps.Copy(c[attr], labelSet)
	}
}

// ClassifySamples uses the provided classifiers to classify the sample data
// passed via the "samples" parameter. It is mostly a helper function which
// loops through each repository.Sample, retrieves the attribute names and
// values of that sample, passes them to Classifier.Classify, and then
// aggregates the results. Please see the documentation for Classifier and its
// Classify method for more details. The returned slice represents all the
// unique classification results for a given sample set.
func ClassifySamples(
	ctx context.Context,
	samples []repository.Sample,
	classifier Classifier,
) ([]ClassifiedTable, error) {
	tables := make([]ClassifiedTable, 0, len(samples))
	for _, sample := range samples {
		// Classify each sampled row and combine the results.
		result := make(Result)
		for _, sampleResult := range sample.Results {
			res, err := classifier.Classify(ctx, sampleResult)
			if err != nil {
				return nil, fmt.Errorf("error classifying sample: %w", err)
			}
			result.Merge(res)
		}
		if len(result) > 0 {
			table := ClassifiedTable{
				Repo:            sample.Metadata.Repo,
				Database:        sample.Metadata.Database,
				Schema:          sample.Metadata.Schema,
				Table:           sample.Metadata.Table,
				Classifications: result,
			}
			tables = append(tables, table)
		}
	}
	return tables, nil
}
