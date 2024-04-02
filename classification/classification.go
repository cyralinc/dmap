// Package classification provides various types and functions to facilitate
// data classification. The type Classifier provides an interface which takes
// arbitrary data as input and returns a classified version of that data as
// output. The package contains at least one implementation which uses OPA and
// Rego to perform the actual classification logic (see LabelClassifier),
// however other implementations may be added in the future.
package classification

import (
	"context"
	"maps"
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
